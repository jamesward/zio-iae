# zio-iae

Infrastructure as Effects for ZIO: declare the cloud resources a program uses as typed ZIO requirements
(`Bucket`, `Queue`, ...), and let an always-on agent plan, approve and apply them on AWS.

Status: early. See [`SPEC.md`](SPEC.md) for the design and milestones.

## Build

```bash
./sbta "Test / compile; testFull; stage"
```

## License

Apache 2.0
