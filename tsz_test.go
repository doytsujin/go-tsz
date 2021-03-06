package tsz

import "testing"

func TestExampleEncoding(t *testing.T) {

	// Example from the paper
	s := New()

	s.Push(12)
	s.Push(12)
	s.Push(24)

	// extra tests

	// floating point masking/shifting bug
	s.Push(13)
	s.Push(24)

	// delta-of-delta sizes
	s.Push(24)
	s.Push(24)
	s.Push(24)

	s.Finish()

	it := s.Iter()

	want := []float64{
		12,
		12,
		24,

		13,
		24,

		24,
		24,
		24,
	}

	for _, w := range want {
		if !it.Next() {
			t.Fatalf("Next()=false, want true")
		}
		vv := it.Values()
		if w != vv {
			t.Errorf("Values()=(%v), want (%v)\n", vv, w)
		}
	}

	if it.Next() {
		t.Fatalf("Next()=true, want false")
	}

	if err := it.Err(); err != nil {
		t.Errorf("it.Err()=%v, want nil", err)
	}
}

var TwoHoursData = []struct {
	v float64
}{
	// 2h of data
	{761}, {727}, {763}, {706}, {700},
	{679}, {757}, {708}, {739}, {707},
	{699}, {740}, {729}, {766}, {730},
	{715}, {705}, {693}, {765}, {724},
	{799}, {761}, {737}, {766}, {756},
	{719}, {722}, {801}, {747}, {731},
	{742}, {744}, {791}, {750}, {759},
	{809}, {751}, {705}, {770}, {792},
	{727}, {762}, {772}, {721}, {748},
	{753}, {744}, {716}, {776}, {659},
	{789}, {766}, {758}, {690}, {795},
	{770}, {758}, {723}, {767}, {765},
	{693}, {706}, {681}, {727}, {724},
	{780}, {678}, {696}, {758}, {740},
	{735}, {700}, {742}, {747}, {752},
	{734}, {743}, {732}, {746}, {770},
	{780}, {710}, {731}, {712}, {712},
	{741}, {770}, {770}, {754}, {718},
	{670}, {775}, {749}, {795}, {756},
	{741}, {787}, {721}, {745}, {782},
	{765}, {780}, {811}, {790}, {836},
	{743}, {858}, {739}, {762}, {770},
	{752}, {763}, {795}, {792}, {746},
	{786}, {785}, {774}, {786}, {718},
}

func TestRoundtrip(t *testing.T) {

	s := New()
	for _, p := range TwoHoursData {
		s.Push(p.v)
	}
	s.Finish()

	it := s.Iter()
	for _, w := range TwoHoursData {
		if !it.Next() {
			t.Fatalf("Next()=false, want true")
		}
		vv := it.Values()
		// t.Logf("it.Values()=(%+v, %+v)\n", time.Unix(int64(tt), 0), vv)
		if w.v != vv {
			t.Errorf("Values()=(%v), want (%v)\n", vv, w.v)
		}
	}

	if it.Next() {
		t.Fatalf("Next()=true, want false")
	}

	if err := it.Err(); err != nil {
		t.Errorf("it.Err()=%v, want nil", err)
	}
}

func BenchmarkEncode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s := New()
		for _, tt := range TwoHoursData {
			s.Push(tt.v)
		}
		s.Finish()
	}
}

func BenchmarkDecode(b *testing.B) {
	s := New()
	for _, tt := range TwoHoursData {
		s.Push(tt.v)
	}
	s.Finish()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		it := s.Iter()
		for j := 0; j < len(TwoHoursData); it.Next() {
			j++
		}
	}
}
