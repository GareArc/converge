module github.com/GareArc/converge/examples

go 1.25

require (
	github.com/GareArc/converge v0.0.0
	github.com/GareArc/converge/adapters/redis v0.0.0
	github.com/redis/go-redis/v9 v9.7.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
)

replace github.com/GareArc/converge => ..

replace github.com/GareArc/converge/adapters/redis => ../adapters/redis
