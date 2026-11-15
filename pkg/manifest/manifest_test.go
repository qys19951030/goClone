package manifest

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func newManifest(t *testing.T) *Manifest {
	t.Helper()
	m, err := Open(context.Background(), filepath.Join(t.TempDir(), "manifest.jsonl"))
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func rec(url, typ, status string) Record {
	return Record{URL: url, Type: typ, Path: "p/" + typ, Status: status}
}

func TestOpen_EmptyFirstRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.jsonl")

	m, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if m.Count() != 0 {
		t.Fatalf("fresh manifest must have zero records, got %d", m.Count())
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if fi, err := os.Stat(path); err != nil || fi.IsDir() {
		t.Fatalf("manifest file not created: %v", err)
	}
}

func TestOpen_ReadBackSuccessSkipsAndFailedReclaimable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.jsonl")
	seed := []Record{
		{URL: "https://e.com/a.png", Type: TypeImg, Path: "assets/img/a.png", Time: "2026-01-01T00:00:00Z", Status: StatusSuccess},
		{URL: "https://e.com/b.js", Type: TypeJS, Path: "assets/js/b.js", Time: "2026-01-01T00:00:01Z", Status: StatusFailed},
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o666)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	for _, r := range seed {
		if err := enc.Encode(r); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()

	m, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if m.Count() != 2 {
		t.Fatalf("count = %d, want 2", m.Count())
	}
	if !m.IsSuccess("https://e.com/a.png") {
		t.Fatalf("successful record must read back as success")
	}
	if m.IsSuccess("https://e.com/b.js") {
		t.Fatalf("failed record must not read back as success")
	}
	// Successful on-disk item: neither claimable nor re-downloaded.
	if m.Claim("https://e.com/a.png") {
		t.Fatalf("success item from disk must not be claimable")
	}
	// Failed item is reprocessed.
	if !m.Claim("https://e.com/b.js") {
		t.Fatalf("failed item must be claimable for retry")
	}
}

func TestPut_LatestStatusWinsAndCountOnlyGrows(t *testing.T) {
	m := newManifest(t)
	url := "https://e.com/x.css"

	if err := m.Put(rec(url, TypeCSS, StatusFailed)); err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := m.Put(rec(url, TypeCSS, StatusSuccess)); err != nil {
		t.Fatalf("put success: %v", err)
	}
	r, ok := m.Get(url)
	if !ok || r.Status != StatusSuccess {
		t.Fatalf("latest record must win, got %+v ok=%v", r, ok)
	}
	if m.Count() != 1 {
		t.Fatalf("same url must keep one entry, count=%d", m.Count())
	}

	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Disk keeps both history lines; read-back collapses to the latest.
	lines := countNonEmptyLines(t, m.path)
	if lines != 2 {
		t.Fatalf("disk must keep append history, lines=%d", lines)
	}
	m2, err := Open(context.Background(), m.path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer m2.Close()
	got, ok := m2.Get(url)
	if !ok || got.Status != StatusSuccess {
		t.Fatalf("read-back must pick latest, got %+v", got)
	}
	if m2.Count() != 1 {
		t.Fatalf("cumulative distinct count = %d, want 1", m2.Count())
	}
}

func TestClaim_InMemoryDedupMatchesDisk(t *testing.T) {
	m := newManifest(t)
	url := "https://e.com/new.png"

	// First claim in memory wins; the second producer must not download.
	if !m.Claim(url) {
		t.Fatalf("first claim should win")
	}
	if m.Claim(url) {
		t.Fatalf("second claim must be deduped in memory")
	}
	if err := m.Put(rec(url, TypeImg, StatusSuccess)); err != nil {
		t.Fatalf("put: %v", err)
	}
	// After persistence, a fresh manifest over the same file must also reject it.
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	m2, err := Open(context.Background(), m.path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if m2.Claim(url) {
		t.Fatalf("url persisted as success must be deduped after restart")
	}
	if !m2.IsSuccess(url) {
		t.Fatalf("IsSuccess should be true after restart")
	}
}

func TestConcurrentWrites_NoInterleaving(t *testing.T) {
	m := newManifest(t)

	const producers = 16
	const perProducer = 40
	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := range perProducer {
				url := fmt.Sprintf("https://e.com/p%d/f%d.png", p, i)
				if !m.Claim(url) {
					t.Errorf("claim rejected fresh url %s", url)
					return
				}
				if err := m.Put(rec(url, TypeImg, StatusSuccess)); err != nil {
					t.Errorf("put %s: %v", url, err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Every physical line must be a complete, standalone JSON record: proof that
	// concurrent batch appends never interleaved.
	f, err := os.Open(m.path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	seen := make(map[string]bool)
	lines := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}
		var r Record
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatalf("line %d is not a complete record: %v (%q)", lines, err, line)
		}
		if seen[r.URL] {
			t.Fatalf("duplicate physical line for %s", r.URL)
		}
		seen[r.URL] = true
		lines++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if lines != producers*perProducer {
		t.Fatalf("lines = %d, want %d", lines, producers*perProducer)
	}
	if m.Count() != producers*perProducer {
		t.Fatalf("memory count = %d, want %d", m.Count(), producers*perProducer)
	}
}

func TestOpen_DropsIncompleteLastLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.jsonl")
	good, _ := json.Marshal(Record{
		URL: "https://e.com/a.js", Type: TypeJS, Path: "a.js",
		Time: "2026-01-01T00:00:00Z", Status: StatusSuccess,
	})
	// Good line, then a truncated fragment without newline.
	content := append(append(good, '\n'), []byte(`{"url":"https://e.com/b.js","type":"js","pa`)...)
	if err := os.WriteFile(path, content, 0o666); err != nil {
		t.Fatal(err)
	}

	// Capture stderr: dropped count must be reported there.
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w

	m, err := Open(context.Background(), path)

	_ = w.Close()
	os.Stderr = old
	if err != nil {
		t.Fatalf("open must tolerate trailing fragment: %v", err)
	}
	defer m.Close()

	stderr, _ := readAll(r)
	if !strings.Contains(stderr, "dropped 1") {
		t.Fatalf("stderr must mention dropped 1 line, got %q", stderr)
	}
	if m.Count() != 1 {
		t.Fatalf("good lines must survive, count=%d", m.Count())
	}
	if !m.IsSuccess("https://e.com/a.js") {
		t.Fatalf("good record lost while dropping fragment")
	}

	// After a clean append and reopen, nothing is dropped and the file stays parseable.
	if err := m.Put(rec("https://e.com/c.png", TypeImg, StatusSuccess)); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	m2, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if m2.Count() != 2 {
		t.Fatalf("count after resume = %d, want 2", m2.Count())
	}
}

func TestOpen_UnwritableFailsBeforeCrawl(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission model differs on windows")
	}
	// Root bypasses ordinary permission bits; simulate with a read-only directory.
	if os.Geteuid() == 0 {
		dir := t.TempDir()
		// Path whose parent exists but is a regular file: open must fail regardless of uid.
		blocker := filepath.Join(dir, "blocker")
		if err := os.WriteFile(blocker, []byte("x"), 0o666); err != nil {
			t.Fatal(err)
		}
		manifestPath := filepath.Join(blocker, "manifest.jsonl")
		if _, err := Open(context.Background(), manifestPath); err == nil {
			t.Fatal("expected failure when parent is not a directory")
		}
		return
	}

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	if _, err := Open(context.Background(), filepath.Join(dir, "manifest.jsonl")); err == nil {
		t.Fatal("expected failure on unwritable manifest path")
	}
}

func TestOpen_PathIsDirectoryFails(t *testing.T) {
	dir := t.TempDir()
	// Manifest path itself is an existing directory.
	if _, err := Open(context.Background(), dir); err == nil {
		t.Fatal("expected failure when manifest path is a directory")
	}
}

func TestPut_AfterWriterFailureReturnsError(t *testing.T) {
	m := newManifest(t)
	if err := m.Put(rec("https://e.com/ok.png", TypeImg, StatusSuccess)); err != nil {
		t.Fatalf("initial put: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Break the writer by closing its file descriptor underneath it, then reopen via Open
	// and sabotage the handle: subsequent Puts must surface the error, never vanish.
	m2, err := Open(context.Background(), m.path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := m2.file.Close(); err != nil {
		t.Fatalf("sabotage close: %v", err)
	}
	putErr := m2.Put(rec("https://e.com/lost.png", TypeImg, StatusSuccess))
	if putErr == nil {
		// The error may surface either synchronously or on Close; either way Close must fail.
		if err := m2.Close(); err == nil {
			t.Fatal("write failure must make Close return an error, records must not be silently lost")
		}
	} else if err := m2.Close(); err != nil && !errors.Is(err, putErr) {
		t.Fatalf("close error %v inconsistent with put error %v", err, putErr)
	}
}

func TestPut_RejectsInvalidRecords(t *testing.T) {
	m := newManifest(t)
	bad := []Record{
		{URL: "", Type: TypeImg, Status: StatusSuccess},
		{URL: "https://e.com/x", Type: "bogus", Status: StatusSuccess},
		{URL: "https://e.com/x", Type: TypeImg, Status: "weird"},
	}
	for i, r := range bad {
		if err := m.Put(r); err == nil {
			t.Fatalf("case %d: invalid record accepted", i)
		}
	}
	if m.Count() != 0 {
		t.Fatalf("invalid records must not enter memory view, count=%d", m.Count())
	}
}

func TestPut_TimestampsAndBatching(t *testing.T) {
	m := newManifest(t)
	// Fewer than flushBatchSize records must still land promptly via the interval flush.
	if err := m.Put(rec("https://e.com/small.js", TypeJS, StatusSuccess)); err != nil {
		t.Fatalf("put: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f, err := os.Open(m.path)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(f)
		n := 0
		for sc.Scan() {
			if len(strings.TrimSpace(sc.Text())) > 0 {
				n++
			}
		}
		_ = f.Close()
		if n == 1 {
			var r Record
			data, _ := os.ReadFile(m.path)
			if err := json.Unmarshal(bytesTrimNewline(data), &r); err != nil {
				t.Fatalf("parse: %v", err)
			}
			if r.Time == "" {
				t.Fatal("timestamp must be filled automatically")
			}
			if _, err := time.Parse(time.RFC3339Nano, r.Time); err != nil {
				t.Fatalf("timestamp not RFC3339Nano: %v", err)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("small batch was not flushed within interval")
}

func TestClose_FlushesQueuedRecords(t *testing.T) {
	m := newManifest(t)
	const n = flushBatchSize + 7
	for i := range n {
		if err := m.Put(rec(fmt.Sprintf("https://e.com/%d.png", i), TypeImg, StatusSuccess)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := countNonEmptyLines(t, m.path); got != n {
		t.Fatalf("flushed lines = %d, want %d", got, n)
	}
}

func countNonEmptyLines(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	n := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		if len(strings.TrimSpace(sc.Text())) > 0 {
			n++
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return n
}

func readAll(f *os.File) (string, error) {
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := f.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			return sb.String(), nil
		}
	}
}

func bytesTrimNewline(b []byte) []byte {
	return []byte(strings.TrimSpace(string(b)))
}
