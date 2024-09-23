package main

import "testing"

type parseSizeTest struct {
	arg string
	val uint64
	err string
}

func TestParseSize(t *testing.T) {
	tests := []parseSizeTest{
		{"5MB", 5_000_000, ""},
		{"5GB", 5_000_000_000, ""},
		{"5TB", 5_000_000_000_000, ""},
		{"5.1GB", 5_100_000_000, ""},
		{"5M", 5_000_000, ""},
		{"5G", 5_000_000_000, ""},
		{"5T", 5_000_000_000_000, ""},
		{"5XB", 0, "invalid syntax"}, // invalid suffix
		{"5", 5, ""},
	}
	// func parseSize(arg string) (uint64, error)

	for i, testCase := range tests {
		t.Run(testCase.arg, func(st *testing.T) {
			val, err := parseSize(testCase.arg)
			if val != testCase.val {
				st.Errorf("[%d] %q val wanted %d but got %d", i, testCase.arg, testCase.val, val)
			}
			if err == nil {
				if testCase.err != "" {
					st.Errorf("[%d] %q err nil but wanted %s", i, testCase.arg, testCase.err)
				}
			} else {
				if testCase.err == "" {
					st.Errorf("[%d] %q err %s", i, testCase.arg, err)
				}
			}
		})
	}
}
