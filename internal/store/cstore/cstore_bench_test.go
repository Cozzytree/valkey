//go:build cstore

package cstore

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/valkey/valkey/internal/store"
)

// newGoStore and newCStore return store.Store so benchmarks use identical code paths.
func newGoStore() store.Store { return store.NewGoStore() }
func newCStore() store.Store { return New() }

type storeFactory struct {
	name string
	fn   func() store.Store
}

var backends = []storeFactory{
	{"GoStore", newGoStore},
	{"CStore", newCStore},
}

// ─── SET ────────────────────────────────────────────────────────────────────

func BenchmarkCompare_Set(b *testing.B) {
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			s := backend.fn()
			defer s.Close()
			val := []byte("hello-world-value")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Set(fmt.Sprintf("key:%d", i), val)
			}
		})
	}
}

// ─── GET hit ────────────────────────────────────────────────────────────────

func BenchmarkCompare_Get_Hit(b *testing.B) {
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			s := backend.fn()
			defer s.Close()
			s.Set("key", []byte("value"))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Get("key")
			}
		})
	}
}

// ─── GET miss ───────────────────────────────────────────────────────────────

func BenchmarkCompare_Get_Miss(b *testing.B) {
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			s := backend.fn()
			defer s.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Get("missing")
			}
		})
	}
}

// ─── SET with TTL ───────────────────────────────────────────────────────────

func BenchmarkCompare_SetWithTTL(b *testing.B) {
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			s := backend.fn()
			defer s.Close()
			val := []byte("hello-world-value")
			ttl := 60 * time.Second
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.SetWithTTL(fmt.Sprintf("key:%d", i), val, ttl)
			}
		})
	}
}

// ─── GET with TTL (not expired) ─────────────────────────────────────────────

func BenchmarkCompare_Get_WithTTL(b *testing.B) {
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			s := backend.fn()
			defer s.Close()
			s.SetWithTTL("key", []byte("value"), 60*time.Second)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Get("key")
			}
		})
	}
}

// ─── DEL ────────────────────────────────────────────────────────────────────

func BenchmarkCompare_Del(b *testing.B) {
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			s := backend.fn()
			defer s.Close()
			for i := 0; i < b.N; i++ {
				s.Set(fmt.Sprintf("key:%d", i), []byte("v"))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Del(fmt.Sprintf("key:%d", i))
			}
		})
	}
}

// ─── TTL ────────────────────────────────────────────────────────────────────

func BenchmarkCompare_TTL(b *testing.B) {
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			s := backend.fn()
			defer s.Close()
			s.SetWithTTL("key", []byte("value"), 60*time.Second)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.TTL("key")
			}
		})
	}
}

// ─── Expire ─────────────────────────────────────────────────────────────────

func BenchmarkCompare_Expire(b *testing.B) {
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			s := backend.fn()
			defer s.Close()
			s.Set("key", []byte("value"))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Expire("key", 60*time.Second)
			}
		})
	}
}

// ─── Concurrent SET (100 goroutines) ────────────────────────────────────────

func BenchmarkCompare_ConcurrentSet(b *testing.B) {
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			s := backend.fn()
			defer s.Close()
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
		})
	}
}

// ─── Concurrent GET (100 goroutines) ────────────────────────────────────────

func BenchmarkCompare_ConcurrentGet(b *testing.B) {
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			s := backend.fn()
			defer s.Close()
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
		})
	}
}

// ─── Concurrent Mixed 75R/25W (100 goroutines) ─────────────────────────────

func BenchmarkCompare_ConcurrentMixed(b *testing.B) {
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			s := backend.fn()
			defer s.Close()
			val := []byte("value")
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
						s.Set(k, val)
					} else {
						s.Get(k)
					}
					i++
				}
			})
		})
	}
}

// ─── Memory per key ─────────────────────────────────────────────────────────

func BenchmarkCompare_Memory(b *testing.B) {
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			for iter := 0; iter < b.N; iter++ {
				s := backend.fn()
				var m1, m2 runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&m1)
				for i := 0; i < 100000; i++ {
					s.Set(fmt.Sprintf("key:%08d", i), []byte("value-data-here!"))
				}
				runtime.GC()
				runtime.ReadMemStats(&m2)
				b.ReportMetric(float64(m2.HeapAlloc-m1.HeapAlloc)/100000.0, "bytes/key")
				s.Close()
			}
		})
	}
}

// ─── ExpireN active expiration ──────────────────────────────────────────────

func BenchmarkCompare_ExpireN(b *testing.B) {
	for _, backend := range backends {
		b.Run(backend.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				s := backend.fn()
				for j := 0; j < 100; j++ {
					s.SetWithTTL(fmt.Sprintf("k:%d", j), []byte("v"), 1*time.Nanosecond)
				}
				time.Sleep(time.Microsecond)
				b.StartTimer()
				s.ExpireN(100)
				b.StopTimer()
				s.Close()
			}
		})
	}
}
