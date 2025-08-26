# Use Sia as a CDN from your web app

1: Make sure you have Rust installed
2. Install `s5_cli` by running `cargo install s5_cli --git https://github.com/s5-dev/s5-rs.git`
3. Run `s5_cli config init` to generate a local keypair and create a config file for your local node
4. Edit the config file at the path printed out by the previous command with your text editor of choice and add the following lines (adjust values as needed):

```toml
[store.default]
type = "sia_renterd"
bucket = "s5-blobs" # TODO make sure you created a bucket with that name in the renterd web ui
worker_api_url = "http://localhost:9980/api/worker"
bus_api_url = "http://localhost:9980/api/bus"
password = "password" # TODO adjust these values as needed
```

5. Import files and directory structures you want to serve from Sia using the `s5_cli import` command. As an example, adding all talk recordings from the WHY2025 conference in h246-hd format would look like this: `s5_cli import https://mirror.netcologne.de/CCC/events/why2025/h264-hd/`

6. Start a local s5 node serving metadata for all imported blobs by running `s5_cli start`

> The instructions below don't work yet due to the s5 blobs protocol still being finalized (to be finished by 2025-09-02)

7. In your web application add the S5 proxy service worker (see the `s5_streamer` crate for instructions)
8. Copy the node id printed in step 6 and paste it in the service worker's config map
9. Now you can stream any file uploaded to your node (or available elsewhere on the network) by for example creating a `video` element with the src `/s5/blob/blobidentifierhere?mediaType=video/mp4` you can get the blob identifier of a file with the `blobsum` cli (`cargo install blobsum`) or by listing all imported files in your local node (`s5_cli ls imported_files`, TODO this is not implemented yet).

