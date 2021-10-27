# akka-persistence-bigtable (Java API)
A journal and snapshot store plugin for `akka-persistence` using [Cloud Bigtable](https://cloud.google.com/bigtable?hl=ru).

## Build
Simple build way:
```
 ./mvnw clean install
```
> ###### Docker required
> To conduct unit tests, the Bigtable emulator is raised in Docker.

Tests can be skipped:
```
 ./mvnw clean install -DskipTests
```

## Test
Tested with [Akka Plugin TCK (Technology Compatibility Kit)](https://doc.akka.io/docs/akka/current/persistence-journals.html#plugin-tck).

Docker image [google/cloud-sdk](https://hub.docker.com/r/google/cloud-sdk) is used as Bigtable emulator.

## Configuration
#### reference.conf
```yaml
akka.persistence.journal.plugin ="bigtable-journal"
akka.persistence.snapshot-store.plugin = "bigtable-snapshot-store"

bigtable-journal {
  class = "com.github.dao.bigtable.persistence.journal.BigtableWriteJournal"
  table = "journal"
  family = "family"
}

bigtable-snapshot-store {
  class = "com.github.dao.bigtable.persistence.snapshot.BigtableSnapshotStore"
  table = "snapshot"
  family = "family"
}

bigtable {
  project-id = "project-id"
  instance-id = "instance-id"
#   emulator-port = 8086
}
```
#### Service Account
You need to define an environment variable `GOOGLE_APPLICATION_CREDENTIALS`
```
GOOGLE_APPLICATION_CREDENTIALS=/path_to_json
```
