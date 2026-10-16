package main

import (
	"flag"
	"io"
	"testing"
	"time"
)

func TestResolveIdleTimeout(t *testing.T) {
	cases := []struct {
		args    []string
		want    time.Duration
		wantErr bool
	}{
		{nil, 60 * time.Second, false},
		{[]string{"--idle-timeout=5s"}, 5 * time.Second, false},
		{[]string{"--idle-timeout=0"}, 0, false},
		{[]string{"--idle-timeout=-1s"}, 0, true},
	}
	for _, c := range cases {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		v := fs.Duration("idle-timeout", 0, "")
		if err := fs.Parse(c.args); err != nil {
			t.Fatal(err)
		}
		got, err := resolveIdleTimeout(fs, 60*time.Second, *v)
		if (err != nil) != c.wantErr || got != c.want {
			t.Fatalf("%v: got %v, %v; want %v err=%v", c.args, got, err, c.want, c.wantErr)
		}
	}
}
