# Go Queue

A lightweight, reliable job queue implemented in Python using Redis Streams, Hash/KV, and Sorted Sets.

Modelled with a strong separation of concerns:
- JobQueue: Manages data persistence, status indexing, retry logic, and Redis communication.
- Worker: Serves as a pure execution engine handling process isolation, graceful shutdowns, signal handling, and retry/reclaim triggers