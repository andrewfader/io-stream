# HTTP read benchmark

Run `bundle install` and `bundle exec ruby http.rb` from `benchmark/`.
For a smoke run: `SAMPLES=1 REQUESTS=3 bundle exec ruby http.rb`.

Compare checkouts using the same installed gems and script:

```sh
ruby -I /path/to/main/lib /path/to/benchmark/http.rb
ruby -I /path/to/pr/lib /path/to/benchmark/http.rb
```

The script prints the loaded io-stream path and HTTP parser version. It exercises
protocol-http1's HTTP/1.1 parser and streaming body reader through
IO::Stream::Buffered over TCP. IPv4 (`127.0.0.1`) and IPv6 (`::1`) run all cases,
without an address-family fallback.

Each sample uses one keep-alive connection and sequential GETs. A server thread
returns prebuilt Content-Length or chunked responses (chunks up to 16 KiB).
There are 2,000 requests per sample for 64 B and 16 KiB bodies, and 64 requests
for 1 MiB bodies. Connection setup and ten warm-up requests are excluded from
timing. Warm-up validates contents; every response checks status and body size.
GC remains enabled. Each sample has a 60-second timeout including setup.

Output reports the fastest of five samples, in requests/s and payload MiB/s.
The server shares the client's Ruby process. These local round-trip measurements
include server work, but no TLS, web framework, concurrency, or memory profiling.
