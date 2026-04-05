package store

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

// ─── basic operation throughput ──────────────────────────────────────────────

func BenchmarkSet(b *testing.B) {
	s := NewGoStore()
	val := []byte("hello-world-value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set(fmt.Sprintf("key:%d", i), val)
	}
}

func BenchmarkGet_Hit(b *testing.B) {
	s := NewGoStore()
	s.Set("key", []byte("value"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get("key")
	}
}

func BenchmarkGet_Miss(b *testing.B) {
	s := NewGoStore()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get("missing")
	}
}

func BenchmarkSetWithTTL(b *testing.B) {
	s := NewGoStore()
	val := []byte("hello-world-value")
	ttl := 60 * time.Second
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.SetWithTTL(fmt.Sprintf("key:%d", i), val, ttl)
	}
}

func BenchmarkGet_WithTTL_Hit(b *testing.B) {
	s := NewGoStore()
	s.SetWithTTL("key", []byte("value"), 60*time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get("key")
	}
}

func BenchmarkGet_WithTTL_Expired(b *testing.B) {
	s := NewGoStore()
	// Pre-set an expired key that will be lazily deleted on first Get,
	// then re-insert for next iteration.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s.SetWithTTL("key", []byte("value"), 1*time.Nanosecond)
		time.Sleep(time.Microsecond)
		b.StartTimer()
		s.Get("key")
	}
}

func BenchmarkDel(b *testing.B) {
	s := NewGoStore()
	for i := 0; i < b.N; i++ {
		s.Set(fmt.Sprintf("key:%d", i), []byte("v"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Del(fmt.Sprintf("key:%d", i))
	}
}

func BenchmarkExpire(b *testing.B) {
	s := NewGoStore()
	s.Set("key", []byte("value"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Expire("key", 60*time.Second)
	}
}

func BenchmarkTTL(b *testing.B) {
	s := NewGoStore()
	s.SetWithTTL("key", []byte("value"), 60*time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.TTL("key")
	}
}

func BenchmarkPersist(b *testing.B) {
	s := NewGoStore()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s.SetWithTTL("key", []byte("value"), 60*time.Second)
		b.StartTimer()
		s.Persist("key")
	}
}

// ─── ExpireN (active expiration cycle) ──────────────────────────────────────

func BenchmarkExpireN_AllExpired(b *testing.B) {
	for _, n := range []int{20, 100, 1000} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			s := NewGoStore()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for j := 0; j < n; j++ {
					s.SetWithTTL(fmt.Sprintf("k:%d", j), []byte("v"), 1*time.Nanosecond)
				}
				time.Sleep(time.Microsecond)
				b.StartTimer()
				s.ExpireN(n)
			}
		})
	}
}

// ─── concurrent access ──────────────────────────────────────────────────────

func BenchmarkConcurrentSet(b *testing.B) {
	s := NewGoStore()
	val := []byte("value")
	b.SetParallelism(100)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Set(fmt.Sprintf("key:%d", i), val)
			i++
		}
	})
}

func BenchmarkConcurrentGet(b *testing.B) {
	s := NewGoStore()
	// Pre-populate
	for i := 0; i < 10000; i++ {
		s.Set(fmt.Sprintf("key:%d", i), []byte("value"))
	}
	b.SetParallelism(100)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(fmt.Sprintf("key:%d", i%10000))
			i++
		}
	})
}

func BenchmarkConcurrentMixed(b *testing.B) {
	s := NewGoStore()
	val := []byte("value")
	// Pre-populate
	for i := 0; i < 10000; i++ {
		s.Set(fmt.Sprintf("key:%d", i), val)
	}
	b.SetParallelism(100)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := fmt.Sprintf("key:%d", i%10000)
			if i%4 == 0 {
				s.Set(k, val) // 25% writes
			} else {
				s.Get(k) // 75% reads
			}
			i++
		}
	})
}

// ─── memory usage ───────────────────────────────────────────────────────────

func BenchmarkMemory_Set(b *testing.B) {
	for _, n := range []int{1000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				s := NewGoStore()
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)
				for i := 0; i < n; i++ {
					s.Set(fmt.Sprintf("key:%08d", i), []byte("value-data-here!"))
				}
				runtime.GC()
				runtime.ReadMemStats(&m2)
				b.ReportMetric(float64(m2.HeapAlloc-m1.HeapAlloc)/float64(n), "bytes/key")
				_ = s.Len() // keep s alive
			}
		})
	}
}

func BenchmarkMemory_SetWithTTL(b *testing.B) {
	for _, n := range []int{1000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			ttl := 60 * time.Second
			for iter := 0; iter < b.N; iter++ {
				s := NewGoStore()
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)
				for i := 0; i < n; i++ {
					s.SetWithTTL(fmt.Sprintf("key:%08d", i), []byte("value-data-here!"), ttl)
				}
				runtime.GC()
				runtime.ReadMemStats(&m2)
				b.ReportMetric(float64(m2.HeapAlloc-m1.HeapAlloc)/float64(n), "bytes/key")
				_ = s.Len()
			}
		})
	}
}
