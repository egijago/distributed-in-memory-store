# Consensus Protocol: RAFT

## Deskripsi Singkat

Repositori ini berisi implementasi sederhana dari protokol konsensus RAFT. Implementasi protokol menggunakan bahasa Go, protobuf, dan GRPC. Sementara itu, web client diimplementasi dengan node.js.

## Daftar Fitur

- Protokol konsensus RAFT
- Web client/Management dashboard

## Cara Penggunaan

### RAFT Client

1. Setup Go, protobuf, make
2. Build Binary untuk client

```
$ make build
```

3. Jalankan sebuah node dengan perintah berikut (gantikan port dengan nomor port node dan port web client)

```
$ make node GRPC_PORT=port GRPC_WEB_PORT=port
```

4. Node dapat juga dijalankan dengan command pengujian untuk 6 node

```
$ make node1
$ make node2
$ make node3
$ make node4
$ make node5
$ make node6
```

### Web Client/Dashboard

1. Setup node.js, Pindah ke /client
2. Jalankan setup modul

```
$ npm install
```

3. Jalankan build client

```
$ npm run dev
```

4. Pilih port RAFT client dengan mengisi port (GRPC_WEB_PORT) pada bagian atas, dan tekan ENTER
5. Command yang dapat diterima adalah sebagai berikut

```
> ping
> get <key>
> set <key> <value>
> del <key>
> strln <key>
> append <key> <value>
> reqlog
```

## Kontributor

- Melvin Kent Jonathan (13521052)
- Daniel Egiant Sitanggang (13521056)
- Bill Clinton (13521064)
- Eugene Yap Jin Quan (13521074)
- Jimly Firdaus (13521102)
