# Integrated Conservation Monitoring & Intelligence System (ICMIS)

ICMIS is an edge-first conservation monitoring platform designed to run on a Raspberry Pi 5. It ingests GPS, environmental, camera, acoustic, and eDNA data; stores the resulting records in SQLite with SpatiaLite; runs ONNX inference and genetic analytics; generates LangGraph risk assessments; and exposes the results through a FastAPI API, React web dashboard, WebSocket alerts, and Flutter field client.

This guide describes a full Raspberry Pi OS Lite deployment. Read the configuration section before starting services: several values in `config.yaml` are placeholders that must be replaced with details for your sensors, study area, species, models, network, and security policy.

## Contents

1. [What runs on the Pi](#what-runs-on-the-pi)
2. [Hardware and operating-system requirements](#hardware-and-operating-system-requirements)
3. [Important project rules](#important-project-rules)
4. [Prepare Raspberry Pi OS Lite](#prepare-raspberry-pi-os-lite)
5. [Install operating-system packages](#install-operating-system-packages)
6. [Copy the project and create directories](#copy-the-project-and-create-directories)
7. [Create the Python environment](#create-the-python-environment)
8. [Configure MQTT](#configure-mqtt)
9. [Configuration values you must review](#configuration-values-you-must-review)
10. [Models and data-file formats](#models-and-data-file-formats)
11. [Initialize and verify the database](#initialize-and-verify-the-database)
12. [Build the web dashboard](#build-the-web-dashboard)
13. [Run the platform manually](#run-the-platform-manually)
14. [Run ICMIS automatically at power-on](#run-icmis-automatically-at-power-on)
15. [API, dashboard, streams, and WebSockets](#api-dashboard-streams-and-websockets)
16. [Build and configure the mobile application](#build-and-configure-the-mobile-application)
17. [Backups, updates, logs, and recovery](#backups-updates-logs-and-recovery)
18. [Troubleshooting](#troubleshooting)
19. [Configuration reference](#configuration-reference)

---

## What runs on the Pi

ICMIS is a group of cooperating processes rather than one monolithic script.

| Component | Command | Purpose | Required? |
|---|---|---|---|
| Database bootstrap | `python database/dbManager.py` | Creates the schema and applies numbered migrations | Run once after installation and after updates |
| API | `uvicorn api.main:app --host 0.0.0.0 --port 8000` | REST API, SSE, WebSocket alerts, report jobs, and compiled dashboard | Yes |
| GPS listener | `python inputs/gpsTelemetry.py` | Consumes GPS MQTT messages | If GPS devices are used |
| Environmental listener | `python inputs/environmentalSensors.py` | Consumes environmental MQTT messages | If environmental sensors are used |
| Camera ingestion | `python inputs/camera.py` | Watches camera and drone-upload directories | If camera media is used |
| Vision inference | `python processing/visionInference.py` | Runs the configured vision ONNX model | If camera AI is used |
| Acoustic ingestion | `python inputs/acousticStream.py` | Watches audio folders or records a live input | If acoustic monitoring is used |
| Acoustic inference | `python processing/acousticInference.py` | Classifies prepared acoustic windows | If acoustic AI is used |
| eDNA ingestion | `python inputs/ednaParser.py` | Validates and imports tabular/FASTA/FASTQ samples | If eDNA is used |
| Genetic analytics | `python processing/geneticAnalytics.py` | Computes population-genetic indicators | If eDNA analytics is used |
| Ollama | managed by its installer | Optional local narrative-generation backend | Optional |

The API report queue and WebSocket fan-out are in-process. Redis, Celery, and RQ are not required by the current implementation. Report job metadata is held in memory; completed reports are persisted in SQLite.

After the one-time `sudo bash deploy/installServices.sh` (see [Run ICMIS automatically at power-on](#run-icmis-automatically-at-power-on)), all of the above start by themselves at every boot, and scheduled backups and risk assessments run on timers. You only need the manual commands in this table for development and debugging.

---

## Hardware and operating-system requirements

Recommended Pi:

- Raspberry Pi 5 with 8 GB RAM. A 4 GB model can run the ingestion and API stack, but local LLM use may be constrained.
- Raspberry Pi OS Lite 64-bit (Bookworm or newer).
- A high-endurance microSD card or, preferably, a USB 3/NVMe SSD.
- Reliable power supply and active cooling. Local ONNX and LLM inference can sustain high CPU load.
- Network access during installation.
- Optional USB camera, microphone/hydrophone, GPS gateway, and environmental MQTT devices.

The project requires:

- Python 3.11 or newer.
- SQLite 3.38 or newer, including JSON support.
- SpatiaLite.
- Node.js 18 or newer to build the dashboard.
- Flutter 3.22/Dart 3.3 or newer only on the workstation used to build the mobile app. Flutter does not need to run on the Pi.

Check the Pi architecture and versions:

```bash
uname -m
python3 --version
python3 -c "import sqlite3; print(sqlite3.sqlite_version)"
```

`uname -m` should normally report `aarch64`. If SQLite is older than 3.38, upgrade Raspberry Pi OS before continuing because the schema uses `STRICT` tables and built-in JSON.

---

## Important project rules

### Always run from the repository root

The Python modules open `config.yaml` using a relative path. Run every command from the folder containing `config.yaml`, for example:

```bash
cd /home/icmis/samsungCompRaspPi
source /home/icmis/icmis/venv/bin/activate
python inputs/gpsTelemetry.py
```

Starting a process from `inputs/`, `api/`, or another directory will make it fail to locate `config.yaml`, schema files, or migrations. The systemd examples later in this document set `WorkingDirectory` for this reason.

### Paths beginning with `~`

The Python modules call `expanduser`, so `~/icmis/...` means the home directory of the Linux account running the process. If systemd runs ICMIS as user `icmis`, the database is `/home/icmis/icmis/inputs.db`. Do not initialize the database as one user and run services as another unless all paths and permissions are deliberately shared.

### Configuration naming

Project configuration keys and source filenames follow the existing camelCase convention. Sensor payload and database column names generally remain snake_case because they are external data contracts.

### One deployment user

This guide uses an `icmis` Linux user. You may use the default `pi` user instead, but replace `/home/icmis` and `User=icmis` consistently. Do not mix paths from both accounts.

---

## Prepare Raspberry Pi OS Lite

1. Use Raspberry Pi Imager to write Raspberry Pi OS Lite 64-bit.
2. In the Imager settings:
   - choose a hostname, such as `icmis-pi`;
   - create a non-default user;
   - configure Wi-Fi if needed;
   - enable SSH;
   - set the correct timezone and keyboard layout.
3. Boot the Pi and connect through SSH:

```bash
ssh <user>@<pi-hostname-or-ip>
```

4. Update all packages and reboot:

```bash
sudo apt update
sudo apt full-upgrade -y
sudo reboot
```

5. Reconnect and set a predictable timezone. Incoming sensor timestamps should be UTC ISO-8601 values even if the display timezone is local:

```bash
sudo timedatectl set-timezone Europe/London
timedatectl
```

6. Enable hardware interfaces only when required:

```bash
sudo raspi-config
```

Use **Interface Options** for camera, I2C, SPI, serial, or other attached hardware. A USB camera or microphone does not normally need an interface enabled in `raspi-config`.

---

## Install operating-system packages

These commands incorporate the system packages listed in `setupCommands.txt` and add the build/version-control utilities needed for a clean Pi installation:

```bash
sudo apt update
sudo apt install -y \
  git curl ca-certificates build-essential cmake pkg-config \
  python3 python3-dev python3-venv python3-pip \
  ffmpeg libsndfile1-dev v4l-utils libgl1 \
  libgstreamer1.0-0 portaudio19-dev \
  libgeos-dev libproj-dev proj-bin \
  spatialite-bin libsqlite3-mod-spatialite \
  mosquitto mosquitto-clients avahi-daemon
```

Raspberry Pi OS releases can rename `libglib2.0-0` to `libglib2.0-0t64`. If one name is unavailable, install the other:

```bash
sudo apt install -y libglib2.0-0 || sudo apt install -y libglib2.0-0t64
```

Verify SpatiaLite:

```bash
find /usr/lib -name 'mod_spatialite.so' -print
```

The usual 64-bit Pi path is `/usr/lib/aarch64-linux-gnu/mod_spatialite.so`. The default `config.yaml` value, `mod_spatialite.so`, works when the dynamic linker can find it. If it cannot, set `storage.spatialiteExtension` to the absolute path returned above.

---

## Copy the project and create directories

Choose one installation method.

### Clone from Git

```bash
cd /home/icmis
git clone <your-repository-url> samsungCompRaspPi
cd /home/icmis/samsungCompRaspPi
```

### Copy from another computer

From the other computer:

```bash
scp -r samsungCompRaspPi <user>@<pi-ip>:/home/<user>/
```

Create all runtime directories from `setupCommands.txt`:

```bash
mkdir -p \
  ~/icmis/models \
  ~/icmis/data/cameraTraps \
  ~/icmis/data/droneUploads \
  ~/icmis/data/archive \
  ~/icmis/data/quarantine \
  ~/icmis/data/acoustic \
  ~/icmis/data/edna/incoming \
  ~/icmis/data/edna/quarantine
```

Do not copy `.venv`, `webDashboard/node_modules`, or Windows build artifacts to the Pi. Recreate dependencies on the Pi so native wheels match ARM64 Linux.

---

## Create the Python environment

From the repository root:

```bash
python3 -m venv ~/icmis/venv
source ~/icmis/venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
```

Install the complete dependency set represented in `setupCommands.txt`:

```bash
python -m pip install \
  aiomqtt pillow opencv-python pyyaml numpy scipy \
  soundfile sounddevice pandas openpyxl biopython h3 watchdog \
  onnxruntime aiosqlite \
  "sqlalchemy[asyncio]>=2.0" "pydantic>=2.7" \
  "langgraph>=1.0" "langchain-core>=1.0" langchain-ollama \
  psutil python-dotenv httpx openai anthropic \
  fastapi "uvicorn[standard]" pydantic-settings websockets \
  ruamel.yaml
```

`ruamel.yaml` lets the Settings page rewrite `config.yaml` while keeping every comment and all the formatting.

### llama.cpp is optional

The default local backend is Ollama. Install `llama-cpp-python` only if you change `aiEngine.llmProvider.localBackend` to `llamacpp`:

```bash
CMAKE_ARGS="-DGGML_NATIVE=ON" python -m pip install llama-cpp-python
```

Compiling this on a Pi can take a long time. Prefer an ARM64 wheel when a compatible trusted wheel is available.

### Verify imports

```bash
python -c "import fastapi, h3, numpy, onnxruntime, sqlalchemy, yaml, ruamel.yaml; print('Python dependencies OK')"
python -c "import sqlite3; c=sqlite3.connect(':memory:'); c.enable_load_extension(True); c.load_extension('mod_spatialite'); print('SpatiaLite OK')"
```

If the second command fails, use the absolute extension path found earlier and update `storage.spatialiteExtension`.

---

## Configure MQTT

MQTT carries GPS and environmental readings. The default broker is local (`localhost:1883`).

Enable and start Mosquitto:

```bash
sudo systemctl enable --now mosquitto
sudo systemctl status mosquitto
```

### Development-only anonymous listener

`setupCommands.txt` contains an anonymous listener:

```conf
listener 1883
allow_anonymous true
```

That is convenient on an isolated test network but unsafe on an exposed or shared network. The installer can write it for you with `sudo bash deploy/installServices.sh --mqtt-lan`. To write it by hand instead:

```bash
sudo tee /etc/mosquitto/conf.d/icmis.conf >/dev/null <<'EOF'
listener 1883
allow_anonymous true
EOF
sudo systemctl restart mosquitto
```

For field deployment, configure Mosquitto users/TLS instead and update sensor credentials. Do not expose port 1883 to the public internet.

Smoke-test the broker with two terminals:

```bash
mosquitto_sub -h localhost -t 'icmis/#' -v
```

```bash
mosquitto_pub -h localhost -t 'icmis/test' -m '{"status":"ok"}'
```

---

## Configuration values you must review

> **Most of these can now be set from the Pi Settings page of the web dashboard or the mobile app** after `installServices.sh` has run. See [Configure the Pi from the web dashboard or the mobile app](#configure-the-pi-from-the-web-dashboard-or-the-mobile-app) for what is remote-editable and [Settings that still need SSH](#settings-that-still-need-ssh-for-now) for what is not. Editing `config.yaml` by hand is still supported; run `sudo systemctl restart icmis.target` afterwards.

Back up the configuration before editing:

```bash
cp config.yaml config.yaml.pre-deployment
nano config.yaml
```

YAML indentation matters. Use spaces, not tabs.

### Values every deployment must decide

These values are site- or deployment-specific. Review all of them even if the current default is acceptable.

| Key | What to set |
|---|---|
| `mqtt.brokerHost` | `localhost` if Mosquitto runs on this Pi, otherwise the broker hostname/IP |
| `mqtt.port` | Broker port, normally `1883` without TLS |
| `mqtt.topics.environmental` | Must match the topic published by environmental devices |
| `mqtt.topics.telemetry` | Must match the topic published by GPS devices |
| `storage.databasePath` | Permanent database location; default `~/icmis/inputs.db` is suitable |
| `storage.spatialiteExtension` | `mod_spatialite.so` or the absolute ARM64 extension path |
| `filePaths.cameraTraps` | Folder where camera-trap media arrives |
| `filePaths.droneUploads` | Folder where drone media arrives |
| `filePaths.quarantine` | Folder for invalid media |
| `edna.projectID` | Your real study/project identifier |
| `edna.studyAreaBounds.*` | The real minimum/maximum latitude and longitude of the study; do not leave global bounds for production |
| `api.allowedOrigins` | Every separately hosted web origin allowed to call the API; remove example domains |
| `api.host` / `api.port` | Keep `0.0.0.0:8000` for LAN access unless another service owns that port |
| `.env: ICMIS_API_KEY` | Set a strong random key when API/WebSocket authentication is enabled |

Generate an API key (not needed if you use `deploy/installServices.sh`, which creates `.env` and generates `ICMIS_API_KEY` for you; you can also rotate the key later under Pi Settings → Secrets):

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Create the environment file without committing it:

```bash
cat > .env <<'EOF'
ICMIS_API_KEY=replace-with-the-generated-value
# Set only if cloud fallback is enabled:
# OPENAI_API_KEY=
# ANTHROPIC_API_KEY=
EOF
chmod 600 .env
```

Then set `requireAuth: true` for the routers that should require `X-API-Key`, and set `websocketApi.requireToken: true`. Browsers and mobile clients pass the WebSocket key through the configured `?token=` parameter.

### Values required when vision inference is enabled

| Key | Required action |
|---|---|
| `vision.modelPath` | Copy your model to this location or change the path |
| `vision.inputWidth`, `inputHeight`, `inputLayout` | Match the model input tensor |
| `vision.outputFormat` | Match the detector output, or use `auto` only if supported correctly |
| `vision.classNames` | List every class in exact model class-ID order |
| `vision.threatClasses` | List only labels that should generate high-priority events |
| `vision.inputScale`, `inputZeroPoint` | Required for quantized integer models; leave scale blank for floating-point inputs |
| `vision.confidenceThreshold`, `iouThreshold` | Tune from validation results, not guesses |

Example:

```yaml
vision:
  modelPath: "~/icmis/models/vision.onnx"
  classNames: ["elephant", "ranger", "vehicle", "person", "fire"]
  threatClasses: ["person", "vehicle", "fire"]
```

### Values required when acoustic monitoring is enabled

| Key | Required action |
|---|---|
| `acoustic.defaultDeviceID` | Fallback device name for files not placed in a device subfolder |
| `acoustic.liveEnabled` | `true` only when this Pi records a live device |
| `acoustic.liveDeviceID` | Device ID matching an entry in `acoustic.devices` |
| `acoustic.liveInputSampleRate`, `liveChannels` | Match the actual microphone/hydrophone |
| `acoustic.inference.modelPath` | Acoustic ONNX model path |
| `acoustic.inference.classMapPath` | CSV path with `index,display_name` headers |
| `acoustic.inference.inputFrames`, `inputMelBands`, `inputLayout` | Match model input |
| `acoustic.inference.wildlifeClasses` | Exact class-map labels treated as wildlife |
| `acoustic.inference.threatClasses` | Exact class-map labels treated as threats |
| `acoustic.inference.classThresholds` | Optional validated per-class thresholds |
| `acoustic.devices` | Real device IDs, locations, coordinates, and altitude |

Example device:

```yaml
acoustic:
  devices:
    - deviceID: "hydrophone-01"
      locationID: "river-north"
      latitude: 51.5000
      longitude: -0.1200
      altitude: 0.0
```

List audio devices before enabling live capture:

```bash
python -c "import sounddevice; print(sounddevice.query_devices())"
```

Recorded `.wav`, `.flac`, and `.mp3` files must be put in `~/icmis/data/acoustic/<deviceID>/`.

### Values required when eDNA is enabled

| Key | Required action |
|---|---|
| `edna.approvedLoci` | Map each scientific species name to approved markers/loci |
| `edna.studyAreaBounds` | Real geographic bounds |
| `edna.maxAlleleCount` | Match the expected ploidy/assay |
| `edna.h3Resolution` | Choose spatial resolution; keep related API resolutions compatible |
| `edna.geneticAnalysis.generationTimeYearsBySpecies` | Add the real generation time for each species |
| `edna.geneticAnalysis.generationTimeYears` | Optional global fallback only if scientifically justified |
| `criticalNeThreshold`, `vulnerableNeThreshold` | Set approved conservation thresholds |

Example:

```yaml
edna:
  projectID: "river-restoration-2026"
  studyAreaBounds:
    minLatitude: 51.10
    maxLatitude: 51.90
    minLongitude: -1.20
    maxLongitude: 0.20
  approvedLoci:
    Salmo salar: ["COI", "12S"]
  geneticAnalysis:
    generationTimeYearsBySpecies:
      Salmo salar: 4.0
```

Do not interpret temporal effective-population-size estimates without an appropriate generation time. The current schema does not retain phased multilocus genotypes for linkage-disequilibrium-based `Ne` estimation.

### Values required for meaningful risk analysis

The defaults are algorithmic starting points, not validated ecological policy.

Review:

- `aiEngine.domainWeights`: all six values must be present and sum to `1.0`.
- `aiEngine.indicatorCalculator.scalingBounds`: supply validated metric ranges when defaults do not fit your ecosystem.
- all component weights and threat/class weights.
- `degradationDirections` and `climateDirections`.
- `threatClassWeights`, `zoneMultipliers`, and mortality settings.
- `graphWorkflow.rangerOutposts`: add real response locations to make feasibility scoring meaningful.
- `graphWorkflow.speciesDomainWeights`: optional species-specific six-domain weights; each mapping must sum to `1.0`.
- `criticalCriThreshold`, poaching thresholds, inbreeding settings, and intervention settings.
- `explainabilityPrompts.criticalSubscoreThreshold`.

Example ranger outposts:

```yaml
aiEngine:
  graphWorkflow:
    rangerOutposts:
      - {name: "North Station", latitude: 51.62, longitude: -0.42}
```

### LLM routing values

Choose one mode:

1. `routingMode: "none"` — deterministic fallback only; lowest resource use and no external data transfer.
2. `routingMode: "local"` — local Ollama or llama.cpp only.
3. `routingMode: "cloud"` — cloud provider only; requires a key and an intentional data-governance decision.
4. `routingMode: "auto"` — local first with cloud fallback under configured conditions.

For a fully offline deployment:

```yaml
aiEngine:
  llmProvider:
    routingMode: "local"
    localBackend: "ollama"
    cloudProvider: "none"
```

For no LLM:

```yaml
aiEngine:
  llmProvider:
    routingMode: "none"
    localBackend: "none"
    cloudProvider: "none"
```

Never commit `.env` or cloud API keys. Sending sensitive wildlife locations to a third-party cloud model may be inappropriate; obtain approval before enabling cloud fallback.

### API and WebSocket values

- Keep `api.debug: false` in field deployments.
- Set `api.enableDocs: false` if interactive API docs should not be exposed.
- `api.requireDatabase: true` is safest; `false` allows startup but data endpoints return `503`.
- Keep `api.frontendDirectory: "webDashboard/dist"` unless the dashboard is built elsewhere.
- `api.llmWarmUp`: use `background`, `blocking`, or `off`. `background` avoids delaying startup.
- `telemetryApi.defaultSpeciesID`: set only if unlabelled threats should always trigger an assessment for one species.
- `telemetryApi.threatWebhookUrl`: leave blank unless an approved receiver exists.
- Keep `reportsApi.jobWorkers: 1` on a Pi so multiple LLM jobs do not compete for memory.
- Keep `websocketApi.zoneResolution`, `reportsApi.scopeResolution`, and `edna.h3Resolution` aligned unless the different resolution is deliberate.
- Set `websocketApi.requireToken: true` when `ICMIS_API_KEY` is configured.

### Advanced values usually left at defaults initially

Queue sizes, retry delays, cache TTLs, time-bucket limits, heartbeat timings, pagination limits, inference threads, and polling intervals are operational tuning controls. Change them only after monitoring queue depth, memory, CPU temperature, latency, and dropped data. The Pi 5 defaults intentionally reserve CPU capacity for ingestion.

---

## Models and data-file formats

The repository contains inference code and configuration, but it does **not** contain trained ONNX weights. Prepare/export the models on a workstation, then copy the `.onnx` files and label map to the Pi. Do not rename an unrelated model to make it load: the model's labels, input preprocessing, output tensor, and configuration must agree.

### Vision model

For a smoke test, export Ultralytics' small YOLO11 COCO detector. It can detect general COCO categories (including elephant, giraffe, and zebra), but it is not a species-specific conservation model and will not identify species absent from COCO.

On a workstation with Python, from any working directory:

```bash
python3 -m venv ~/icmis-export-venv
source ~/icmis-export-venv/bin/activate
python -m pip install --upgrade pip
python -m pip install ultralytics
yolo export model=yolo11n.pt format=onnx imgsz=640 opset=12 nms=False dynamic=False
```

Ultralytics downloads `yolo11n.pt` the first time. The export command creates `yolo11n.onnx` in the current directory. Copy it to the configured model path on the Pi:

```bash
scp yolo11n.onnx <pi-user>@<pi-host>:~/icmis/models/vision.onnx
```

This export's fixed `640 x 640` RGB input, NCHW layout, normalized pixel values, and raw YOLO detection output match the current vision preprocessor/decoder. Keep `vision.outputFormat: "auto"` (or set it to `"yolov8"`); do not enable an export-time NMS wrapper because the ICMIS decoder performs its own non-maximum suppression.

The COCO model has 80 classes. `vision.classNames` must contain **all** model class names in model class-index order; a short list containing only the animal names gives wrong labels for the other class IDs. To inspect the names bundled with the downloaded weights on the workstation:

```bash
python -c "from ultralytics import YOLO; print(YOLO('yolo11n.pt').names)"
```

Copy those names into `vision.classNames` in `config.yaml`. For actual target species or local threats, use a legally suitable labelled image dataset and fine-tune a detector instead of relying on COCO:

```bash
yolo detect train model=yolo11n.pt data=/path/to/dataset.yaml imgsz=640 epochs=100
yolo export model=/path/to/runs/detect/train/weights/best.pt format=onnx imgsz=640 opset=12 nms=False dynamic=False
```

Set `vision.classNames` to the trained dataset's class names in ID order, and set `vision.threatClasses` to exact names from that list. Use the actual training run's `best.pt` path if it is not `runs/detect/train/weights/best.pt`. Validate the resulting detections on held-out images from the intended cameras before treating alerts as reliable.

Check Ultralytics' [export instructions](https://docs.ultralytics.com/modes/export/), [YOLO11 documentation](https://docs.ultralytics.com/models/yolo11/), and [license](https://github.com/ultralytics/ultralytics/blob/main/LICENSE) before deployment or redistribution. Ultralytics' code and pretrained weights are AGPL-3.0; make sure that license is suitable for your use.

Supported camera media includes common image formats (`jpg`, `jpeg`, `png`, `webp`, `bmp`, `tiff`) and common video formats (`mp4`, `avi`, `mov`, `mkv`, `webm`, and others). Camera files are watched in:

- `~/icmis/data/cameraTraps`
- `~/icmis/data/droneUploads`

Successfully handled media is archived. Invalid or unsafe input is quarantined. Metadata without location may be retained with a missing-location state but cannot contribute correctly to spatial analysis.

### Acoustic model

There is no ready-to-use acoustic ONNX model in this repository, and a meaningful one cannot be created from the inference code alone: it needs labelled recordings for the wildlife/threat classes at your site. Generic YAMNet or BirdNET downloads are **not** drop-in replacements for this interface. This pipeline resamples mono audio to 16 kHz, creates its own 64-band log-mel features, and feeds a resized spectrogram to the model; those pretrained models use different audio inputs and outputs.

To obtain a compatible model:

1. Choose the labels you actually need and collect licensed, labelled recordings representative of the deployed microphones, sites, seasons, and background noise. Include negative/background examples. Record the source and usage rights for each dataset.
2. Split recordings into training, validation, and held-out test sets **by recording/site or source**, not by adjacent audio windows, to avoid testing on near-duplicates of training audio.
3. Use the same feature generation as `inputs/acousticStream.py` and tensor preparation as `processing/acousticInference.py` both for training and inference: mono float audio resampled to 16 kHz; 3-second windows; Hann STFT with `nFft: 1024` and `hopLength: 256`; the project's 64-band triangular Mel filterbank and `10 * log10(power)`; then transpose and resize to 96 time frames by 64 Mel bands. The model input is a float32 tensor of shape `[1, 1, 96, 64]` with `acoustic.inference.inputLayout: "NCHW"`.
4. Train a classifier with one independent output per label (shape `[1, C]`). A multi-label loss such as binary cross-entropy with logits is suitable when more than one sound can occur in a window. Include a sigmoid in the exported model so its outputs are probabilities in `[0, 1]`; do not use a softmax when multiple labels may be present.
5. Export that trained classifier to ONNX with a fixed spectrogram shape and a float32 input. Use `onnxruntime` on the workstation to verify that a `[1, 1, 96, 64]` test tensor produces exactly `C` finite probabilities in `[0, 1]`.
6. Copy both files to the Pi, preserving the mapping between output index and label:

```bash
scp /path/to/acoustic.onnx <pi-user>@<pi-host>:~/icmis/models/acoustic.onnx
scp /path/to/acousticClasses.csv <pi-user>@<pi-host>:~/icmis/models/acousticClasses.csv
```

The CSV header must include an index and label column. Indices must be unique, zero-based, and match the model's output positions:

```csv
index,display_name
0,background
1,elephant
2,gunshot
```

In `config.yaml`, keep the acoustic dimensions/layout at `inputFrames: 96`, `inputMelBands: 64`, and `inputLayout: "NCHW"` unless you also change the model and preprocessing to match. Put the model's wildlife labels in `wildlifeClasses`, threat labels in `threatClasses`, and optional per-label cutoffs in `classThresholds`; each value must match a `display_name` in the CSV. Leave `inputScale` unset for a float32 model. Tune thresholds on the held-out test set, especially for threat alerts, and validate false-positive and false-negative rates before operational use.

The earlier design notes in `aiResponse.txt` mention YAMNet and BirdNET as examples of audio classifiers, not as tested or included ICMIS model files. To use either, adapt and test the ICMIS feature pipeline/model adapter to that model's documented input and output rather than simply renaming its file.

### eDNA inputs

Supported formats:

- CSV and TSV
- XLSX
- FASTA (`.fasta`, `.fa`, `.fas`)
- FASTQ (`.fastq`, `.fq`)

Recognized data fields include:

- `sample_id`
- `project_id`
- `target_species`
- `collection_date`
- `latitude`
- `longitude`
- `altitude`
- `locus_name`
- `allele_count`
- `allele_state`
- `sequence`

Common aliases such as `species`, `scientificname`, `lat`, `lng`, `marker`, and `genotype` are normalized. FASTA/FASTQ headers must include species, collection date, latitude, longitude, and locus as `key=value` metadata. Put files in `~/icmis/data/edna/incoming`. Invalid records go to the eDNA quarantine folder.

### MQTT payloads

Publish GPS readings to the configured telemetry topic and environmental readings to the environmental topic. Timestamps should be UTC ISO-8601 values such as `2026-09-28T12:34:56Z`. Latitude must be between `-90` and `90`; longitude must be between `-180` and `180`.

Basic GPS test:

```bash
mosquitto_pub -h localhost -t icmis/telemetry/collar-1 -m \
'{"sensor_id":"collar-1","animal_id":"ele-1","species_id":1,"timestamp":"2026-09-28T12:34:56Z","latitude":-1.3,"longitude":36.8,"altitude":1600,"battery_percentage":95}'
```

---

## Initialize and verify the database

Activate the environment and run from the repository root:

```bash
cd /home/icmis/samsungCompRaspPi
source ~/icmis/venv/bin/activate
python database/dbManager.py
```

The manager:

1. creates the configured SQLite database;
2. loads SpatiaLite;
3. applies `database/schema.sql`;
4. applies numbered migrations from `database/schemaMigrations`;
5. records schema/checksum state.

Migration filenames follow `NNNNdescriptiveName.sql`, for example `0004telemetryTimeIndexes.sql`. Migration files must not contain their own `BEGIN` or `COMMIT`; the manager wraps each one in an exclusive transaction.

The base schema re-runs only when its checksum changes. Changes to existing tables should be introduced through a new migration, not by silently changing deployed DDL.

Verify ORM mappings:

```bash
python database/models.py
```

Back up the new database:

```bash
sqlite3 ~/icmis/inputs.db ".backup '$HOME/icmis/inputs-initial.db'"
```

---

## Build the web dashboard

Install Node.js 20 LTS. One method is NodeSource:

```bash
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
node --version
npm --version
```

Build from the project:

```bash
cd /home/icmis/samsungCompRaspPi/webDashboard
npm install
```

If npm 11 defers esbuild's install script:

```bash
npm approve-scripts esbuild
npm rebuild esbuild
```

Create an optional dashboard environment file:

```bash
cp .env.example .env
nano .env
```

When FastAPI serves the dashboard, leave `VITE_API_BASE_URL` blank so REST and WebSocket requests use the same origin. If API authentication is enabled, set `VITE_API_KEY` to the same `ICMIS_API_KEY`.

Build:

```bash
npm run build
```

The output is `webDashboard/dist`, which FastAPI mounts automatically. Rebuild whenever dashboard source or `VITE_*` build-time settings change.

Development only:

```bash
npm run dev
```

The Vite server uses port `3000` and proxies API/WebSocket traffic to port `8000`.

---

## Optional local Ollama model

Install Ollama and the configured model:

```bash
curl -fsSL https://ollama.com/install.sh | sh
ollama pull llama3.2:3b
```

Verify:

```bash
curl http://localhost:11434/api/tags
```

If the Pi lacks sufficient free RAM, is thermally throttled, or Ollama is unavailable, the provider can use the configured cloud route or deterministic fallback. `aiEngine.llmProvider.minFreeRamGb` defaults to `2.5`, and local inference is blocked above `80°C`.

---

## Run the platform manually

Initialize the database first. Then use separate SSH sessions, `tmux` panes, or systemd services.

```bash
cd /home/icmis/samsungCompRaspPi
source ~/icmis/venv/bin/activate
python database/dbManager.py
```

Start only the ingestion modules for hardware you actually use:

```bash
python inputs/gpsTelemetry.py
python inputs/environmentalSensors.py
python inputs/camera.py
python processing/visionInference.py
python inputs/acousticStream.py
python processing/acousticInference.py
python inputs/ednaParser.py
python processing/geneticAnalytics.py
```

Start the API:

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

Alternatively:

```bash
python api/main.py
```

The latter reads host/port/debug settings from `config.yaml`.

### Recommended startup order

1. Mosquitto.
2. Database initialization/migrations.
3. Ingestion processes.
4. Inference/analytics workers corresponding to those ingestion processes.
5. Ollama, if used.
6. FastAPI.

Do not run the standalone demos in `api/routesTelemetry.py`, `api/routesAnalytics.py`, `api/routesReports.py`, or `api/websocketManager.py` alongside the real API. Those `__main__` blocks are for isolated development checks.

---

## Run ICMIS automatically at power-on

ICMIS is designed to be **zero-touch**: after a one-time install, pressing the Pi's power button (or restoring power after an outage) brings up the database, API, dashboard, every configured worker, and every schedule with no keyboard, SSH session, or user action.

### One-time install

Run this once, from the deployment account (for example `icmis`), after the Python environment exists and the database has been initialised at least once:

```bash
cd /home/icmis/samsungCompRaspPi
sudo bash deploy/installServices.sh
```

Optional flags:

| Flag | Effect |
|---|---|
| `--mqtt-lan` | Writes `/etc/mosquitto/conf.d/icmis.conf` so field sensors on the LAN can publish to port 1883 (anonymous; private networks only) |
| `--python /path/to/python` | Uses a different interpreter than `~/icmis/venv/bin/python` |

The installer is idempotent, so re-run it after every `git pull`. It:

1. Creates `~/icmis/state`, `~/icmis/backups`, `~/icmis/models`, and `~/icmis/configBackups`.
2. Creates `.env` (mode `600`) next to `config.yaml` and, **the first time only**, generates `ICMIS_API_KEY` and prints it. **Copy this key**: the web dashboard and mobile app need it to open the Settings page.
3. Enables Mosquitto, Avahi (so `http://icmis.local` works), and Ollama if it is installed.
4. Makes the journal persistent (200 MB cap) and enables the hardware watchdog so a hung Pi reboots itself.
5. Builds `webDashboard/dist` if it has not been built yet and `npm` is available.
6. Renders every template in `deploy/systemd/` into `/etc/systemd/system/`, with your user, paths, and interpreter filled in. Windows CRLF line endings are stripped automatically.
7. Applies `system.hostname`, `system.timezone`, and the timer schedules from `config.yaml`.
8. Enables and starts everything.

Reboot once to prove the zero-touch start works:

```bash
sudo reboot
# after about a minute, from any computer on the LAN:
curl http://icmis.local:8000/api/v1/health
```

### What starts at boot

| Unit | Kind | What it does |
|---|---|---|
| `icmis.target` | target | Groups every ICMIS unit; `systemctl restart icmis.target` restarts the whole stack |
| `icmis-db.service` | one-shot | Runs `database/dbManager.py` (schema and migrations) before anything else touches the database |
| `icmis-api.service` | continuous | `api/main.py`: REST, SSE, WebSockets, settings, and the compiled dashboard |
| `icmis-worker@gpsTelemetry.service` | continuous | `inputs/gpsTelemetry.py` |
| `icmis-worker@environmentalSensors.service` | continuous | `inputs/environmentalSensors.py` |
| `icmis-worker@camera.service` | continuous | `inputs/camera.py` |
| `icmis-worker@visionInference.service` | continuous | `processing/visionInference.py` |
| `icmis-worker@acousticStream.service` | continuous | `inputs/acousticStream.py` |
| `icmis-worker@acousticInference.service` | continuous | `processing/acousticInference.py` |
| `icmis-worker@ednaParser.service` | continuous | `inputs/ednaParser.py` |
| `icmis-worker@geneticAnalytics.service` | continuous | `processing/geneticAnalytics.py` |
| `icmis-backup.timer` | schedule | Runs `deploy/backupDatabase.py` on `schedules.databaseBackup.onCalendar` (default 02:30 daily) and keeps `keepCopies` copies of the database and `config.yaml` |
| `icmis-assessment.timer` | schedule | Runs `deploy/scheduledAssessment.py` on `schedules.riskAssessment.onCalendar` (default 06:00 daily) for every entry in `schedules.riskAssessment.targets` |
| `icmis-apply.path` | watcher | Watches `~/icmis/state` and starts the root-only `icmis-apply.service` when the dashboard or app asks for settings to be applied |

Continuous units use `Restart=always`, so a crashed worker restarts after 15 seconds (the API after 10). Timers use `Persistent=true`: if the Pi was off at 02:30, the backup runs as soon as it boots.

**Readiness checks instead of crash loops.** Before each worker starts, `deploy/runWorker.py --check <worker>` decides whether it can run. A worker is *skipped* (reported as inactive rather than failed) when:

- it is switched off under `services.<worker>.enabled`;
- `visionInference`: `vision.modelPath` does not exist or `vision.classNames` is empty;
- `acousticInference`: `acoustic.inference.modelPath` or `acoustic.inference.classMapPath` does not exist;
- `ednaParser`: `edna.approvedLoci` is empty.

This means you can leave every worker enabled on day one. Each one starts on its own at the next boot, or at the next apply, once its model or configuration exists. The reason for every skip is shown on the Settings page and by:

```bash
~/icmis/venv/bin/python deploy/runWorker.py --check visionInference
```

### Configure the Pi from the web dashboard or the mobile app

Every setting that an operator should normally change is editable remotely, without SSH:

- **Web dashboard:** open `http://icmis.local:8000/#/settings` (or `http://<pi-ip>:8000/#/settings`) and choose **Pi Settings** in the navigation.
- **Mobile app:** open the **Pi Settings** tab.

**First connection (pairing).** Under **Connection**, enter the Pi address (`icmis.local`, `192.168.1.100`, or a full URL; `:8000/api/v1` is added automatically in the app) and the `ICMIS_API_KEY` printed by the installer. The dashboard stores the key in the browser's local storage. The app stores both values in Hive, so they survive restarts. If you lose the key, read it on the Pi with `grep ICMIS_API_KEY ~/samsungCompRaspPi/.env`.

**How a change reaches the Pi.**

1. **Save** sends only the changed values, together with the revision of `config.yaml` you loaded. If someone else saved in the meantime, you get `409` and the form reloads rather than overwriting their work.
2. The API validates every value (type, range, allowed options, and cross-field checks such as `minimum < maximum` for study bounds).
3. **Pre-flight:** the API imports every affected program against the *candidate* file in a throwaway process. A program that would reject the new values blocks the save and shows its error next to the field.
4. The current `config.yaml` is copied to `~/icmis/configBackups/` (the last `settingsApi.maxBackups` copies are kept). The new file is then written atomically, and your comments and formatting are preserved.
5. A yellow **"Saved changes are waiting to be applied"** banner lists the programs that must restart. **Apply now** writes a request into `~/icmis/state`. `icmis-apply.path` sees it and runs `deploy/applySettings.py` as root, which:
   - restarts only the affected workers;
   - enables or disables workers and timers;
   - rewrites timer schedules;
   - sets the hostname and timezone;
   - restarts the API last.
6. If the API itself restarts, the page and app wait for `/health` to answer again, then reload.

Other controls on the same page:

- **Programs on the Pi:** the live systemd state of the API, every worker, and every timer (next and last run), plus the reason any worker is skipped. Each worker has a **Restart** button.
- **Secrets:** write-only fields for `ICMIS_API_KEY`, `OPENAI_API_KEY`, and `ANTHROPIC_API_KEY`, stored in `.env` and never sent back. Rotating `ICMIS_API_KEY` restarts the API and then switches the device you are using to the new key. Other paired devices must be given the new key.
- **Config backups:** restore any earlier `config.yaml`. The file being replaced is backed up first. Press **Apply now** afterwards.

The **Apply** button and service states only work on the Pi after `installServices.sh` has been run. On a development PC, saving works, but Apply returns `503`.

The settings router is registered with `requireAuth: true` in `api.routerModules`, so every settings request must carry the `X-API-Key` header matching `ICMIS_API_KEY`. Keep that key private: anyone who has it can reconfigure the Pi.

### Settings you can change from the apps

| Group | Keys |
|---|---|
| Pi & network | `system.hostname`, `system.timezone`, `mqtt.brokerHost`, `mqtt.port`, `mqtt.topics.environmental`, `mqtt.topics.telemetry` |
| Programs | `services.<worker>.enabled` for all eight workers |
| Schedules | `schedules.databaseBackup.*` (enabled, onCalendar, keepCopies, directory) and `schedules.riskAssessment.*` (enabled, onCalendar, defaultLookbackDays, targets) |
| Storage | `storage.databasePath`, `filePaths.cameraTraps`, `filePaths.droneUploads`, `filePaths.quarantine`, `acoustic.inputDirectory`, `edna.inputDirectory`, `edna.quarantineDirectory` |
| Sensors & camera | `validation.batteryLowThreshold`, `camera.pollingIntervalSec` |
| Vision model | `vision.modelPath`, input width and height, confidence and IoU thresholds, threads, `classNames`, `threatClasses` |
| Acoustic monitoring | live input (enabled, device, sample rate, channels), `acoustic.devices`, and the model, class map, thresholds, and class lists under `acoustic.inference` |
| eDNA & genetics | `edna.projectID`, `edna.studyAreaBounds`, `edna.approvedLoci`, and the `edna.geneticAnalysis` interval, Ne thresholds, and generation times |
| Risk engine | `aiEngine.domainWeights`, species domain weights, the critical CRI and Ne thresholds, `rangerOutposts` |
| AI language model | routing mode, local backend, Ollama model and URL, GGUF path, threads, temperature and RAM limits, cloud provider and model names |
| Server & alerts | `api.allowedOrigins`, `api.enableDocs`, `api.logLevel`, `api.llmWarmUp`, the telemetry webhook and AI wake-up settings, the WebSocket CRI thresholds, `forwardTelemetry`, `requireToken` |

`onCalendar` uses systemd calendar syntax, for example `*-*-* 02:30:00` (daily), `Mon *-*-* 06:00:00` (weekly), or `*-*-* *:00/15:00` (every 15 minutes). Check an expression on the Pi with `systemd-analyze calendar "*-*-* 02:30:00"`.

### Settings that still need SSH (for now)

These are rarely changed, or changing them remotely could lock you out, so they are intentionally **not** in the Settings UI yet:

| Key | Why it is not remote-editable yet |
|---|---|
| `storage.spatialiteExtension` | A wrong value stops the database from opening, so the API could not start and fix it |
| `api.host`, `api.port` | Changing them would disconnect every client from the API they are talking to |
| `*.requireAuth` on each router | Turning auth off remotely is a security risk; turning it on without a key locks the UI out |
| `settingsApi.*` | The settings system's own state, backup, and pre-flight folders and limits |
| `database.*`, `emaAlphas`, `validation` (other than battery), timeouts, queue sizes, and model tensor names | Advanced tuning that should be tested on a bench first |
| Mosquitto users and TLS | Files under `/etc/mosquitto/` (outside the project) |

Edit these with `nano config.yaml`, then run `sudo systemctl restart icmis.target`.

### Checking on the services

```bash
systemctl list-units 'icmis*' --all          # every ICMIS unit and its state
systemctl list-timers 'icmis*'               # next/last run of each schedule
journalctl -u 'icmis*' -f                    # follow all ICMIS logs
journalctl -u icmis-api -b                   # API log since this boot
journalctl -u icmis-worker@gpsTelemetry -b   # one worker
journalctl -u icmis-apply -n 50              # what the last "Apply now" did
cat ~/icmis/state/applyStatus.json           # machine-readable result of the last apply
sudo systemctl restart icmis.target          # restart everything
sudo systemctl start icmis-backup.service    # take a backup right now
```

To stop ICMIS starting at boot, run `sudo systemctl disable icmis.target`. To re-enable it, run `sudo bash deploy/installServices.sh`.

---

## API, dashboard, streams, and WebSockets

Assuming the Pi IP is `192.168.1.100`:

| Resource | URL |
|---|---|
| Dashboard | `http://192.168.1.100:8000/` |
| Health | `http://192.168.1.100:8000/api/v1/health` |
| API docs | `http://192.168.1.100:8000/docs` |
| Telemetry data | `http://192.168.1.100:8000/api/v1/telemetry/data` |
| SSE telemetry | `http://192.168.1.100:8000/api/v1/telemetry/stream` |
| WebSocket | `ws://192.168.1.100:8000/api/v1/ws/alerts/<client_id>` |

Health check:

```bash
curl http://localhost:8000/api/v1/health
```

Look for:

- `"database": "ok"`
- `"frontend_mounted": true`
- expected router names under `routers_loaded`

Useful API smoke tests:

```bash
curl "http://localhost:8000/api/v1/analytics/summary?window_hours=24"
curl "http://localhost:8000/api/v1/analytics/historical?source=risk&bucket=auto&imputation=forward_fill"
curl "http://localhost:8000/api/v1/reports?limit=20&offset=0"
curl "http://localhost:8000/api/v1/telemetry/data?reading_type=gps&limit=100"
curl -N "http://localhost:8000/api/v1/telemetry/stream?reading_type=gps"
```

Generate a report:

```bash
curl -X POST http://localhost:8000/api/v1/reports/generate \
  -H "Content-Type: application/json" \
  -d '{"species_id":1,"latitude":-1.3,"longitude":36.8,"lookback_days":30}'
```

The response is `202 Accepted` with a `job_id`. Poll:

```bash
curl "http://localhost:8000/api/v1/reports/status/<job_id>?redirect=false"
```

WebSocket clients must answer:

```json
{"type":"ping"}
```

with:

```json
{"type":"pong"}
```

The server drops a connection after the configured number of missed heartbeats. Clients may send `subscribe`, `unsubscribe`, `status`, and `ping` control frames.

Publish a live alert:

```bash
curl -X POST http://localhost:8000/api/v1/ws/alerts \
  -H "Content-Type: application/json" \
  -d '{"priority":"CRITICAL","event_type":"acoustic_anomaly","classification":"gunshot_detected","metrics":{"cri_score":88,"confidence":0.94},"location":{"lat":-2.334,"lng":34.821,"zone":"Sector_4_North"}}'
```

If authentication is enabled, add:

```bash
-H "X-API-Key: $ICMIS_API_KEY"
```

The dashboard uses hash routes because the static mount has no history fallback:

- `http://<pi-ip>:8000/#/`
- `http://<pi-ip>:8000/#/species/1`
- `http://<pi-ip>:8000/#/reports`

---

## Build and configure the mobile application

Build the Flutter app on a development workstation, not on the Pi. The Pi only hosts the API.

Because the checked-in project contains the Dart application and dependency manifest, generate native platform shells once if `android/` and `ios/` are absent:

```bash
cd mobile_app
flutter create . --platforms=android,ios --project-name=icmis_mobile --org=org.icmis
flutter pub get
flutter analyze
flutter test
```

Run on a device connected to the same network:

```bash
flutter run
```

On first launch, open the **Pi Settings** tab and enter the Pi address (`icmis.local` or its IP) and the `ICMIS_API_KEY` printed by the installer, then press **Save & test connection**. Both values are saved on the phone and reused at every launch. The live alert WebSocket reconnects to the new address straight away.

Optionally, bake defaults into a build so a fresh install needs no pairing. Values entered in the app still take priority:

```bash
flutter run \
  --dart-define=ICMIS_API_URL=http://<pi-ip>:8000/api/v1 \
  --dart-define=ICMIS_API_KEY=<api-key>
```

Without `--dart-define`, the app defaults to `http://icmis.local:8000/api/v1`.

Build Android:

```bash
flutter build apk --release \
  --dart-define=ICMIS_API_URL=http://<pi-ip>:8000/api/v1 \
  --dart-define=ICMIS_API_KEY=<api-key>

flutter build appbundle --release \
  --dart-define=ICMIS_API_URL=http://<pi-ip>:8000/api/v1 \
  --dart-define=ICMIS_API_KEY=<api-key>
```

Build iOS on macOS with Xcode:

```bash
flutter build ipa --release \
  --dart-define=ICMIS_API_URL=http://<pi-ip>:8000/api/v1 \
  --dart-define=ICMIS_API_KEY=<api-key>
```

For production, prefer HTTPS/WSS. Android and iOS may block cleartext `http://`/`ws://` depending on platform policy; configure a reverse proxy with TLS rather than globally weakening transport security.

The mobile app currently:

- initializes Hive before rendering;
- caches reports for offline display;
- queues checklist writes when Dio reports a network failure;
- replays queued operations when `flushPendingWrites()` is called;
- consumes WebSocket alerts and answers heartbeat pings;
- reconnects with exponential backoff;
- preserves tab state with `IndexedStack`.

The current map uses online OpenStreetMap tiles. For a truly offline field deployment, add an MBTiles provider and pre-seed approved regional tiles on the device. The comment in `setupCommands.txt` is a deployment reminder; an MBTiles package is not included in this repository.

---

## Backups, updates, logs, and recovery

### Database backups

Backups are automatic. `icmis-backup.timer` runs `deploy/backupDatabase.py` on `schedules.databaseBackup.onCalendar` (default 02:30 daily). It uses SQLite's online backup API, copies `config.yaml` alongside, and keeps the newest `keepCopies` sets in `schedules.databaseBackup.directory` (default `~/icmis/backups`). Change any of these under Pi Settings → Schedules. To take one immediately:

```bash
sudo systemctl start icmis-backup.service
ls -lh ~/icmis/backups
```

Manual equivalent (never copy a live database file with `cp`):

```bash
mkdir -p ~/icmis/backups
sqlite3 ~/icmis/inputs.db ".backup '$HOME/icmis/backups/inputs-$(date +%F-%H%M%S).db'"
```

Retain backups on another physical device. Wildlife and ranger location data is sensitive.

### Updating the code

```bash
cd /home/icmis/samsungCompRaspPi
sudo systemctl stop icmis.target
git pull
source ~/icmis/venv/bin/activate
python -m pip install --upgrade <only-packages-required-by-the-update>
cd webDashboard
npm install
npm run build
cd ..
# Re-renders the systemd units and starts everything again (icmis-db runs the migrations first)
sudo bash deploy/installServices.sh
```

Never replace `config.yaml` or `.env` blindly during an update. Merge new settings while preserving deployment values.

### Logs

```bash
journalctl -u 'icmis*' -f
sudo journalctl -u icmis-api --since today
journalctl -u icmis-worker@gpsTelemetry -b
sudo journalctl -u mosquitto -f
```

The installer makes the journal persistent, so logs from before a reboot or power cut are kept (`journalctl -b -1` shows the previous boot). With manual processes, output goes to the active terminal.

### Safe shutdown

```bash
sudo shutdown -h now
```

systemd sends `SIGINT` to every ICMIS program, and each one drains its queues and closes the database before power-off. Avoid pulling power while SQLite is writing. After a power cut, ICMIS still restarts cleanly at the next boot.

---

## Troubleshooting

### `FileNotFoundError: config.yaml`

The process was not started from the repository root. Run:

```bash
cd /home/icmis/samsungCompRaspPi
```

or set the systemd `WorkingDirectory`.

### SpatiaLite cannot load

Symptoms include `sqlite3.OperationalError` mentioning `mod_spatialite.so`.

```bash
find /usr/lib -name mod_spatialite.so
ldd /usr/lib/aarch64-linux-gnu/mod_spatialite.so
```

Install `libsqlite3-mod-spatialite`, then set `storage.spatialiteExtension` to the returned absolute path if needed.

### Database is locked

- Ensure all configured high-rate API writes use the database manager's queue.
- Do not run duplicate copies of the same ingestion process.
- Keep the database on local SSD/microSD storage, not an unreliable network filesystem.
- Confirm only one installation/user is pointing at the database.
- Do not reduce `busyTimeoutMs` or retry settings without evidence.

### API starts but endpoints return `503`

Check `/api/v1/health`, database initialization, database path permissions, and SpatiaLite loading. With `api.requireDatabase: false`, startup can succeed while database-backed endpoints remain unavailable.

### Dashboard returns 404 or health says `frontend_mounted: false`

```bash
cd webDashboard
npm install
npm run build
ls dist/index.html
```

Confirm `api.frontendDirectory` is `webDashboard/dist` and restart the API.

### Dashboard cannot call the API

- If served by FastAPI, leave `VITE_API_BASE_URL` blank.
- If served separately, add its exact origin to `api.allowedOrigins`.
- Rebuild after changing `VITE_*`.
- Verify firewall rules and API key consistency.

### MQTT receives nothing

```bash
sudo systemctl status mosquitto
mosquitto_sub -h localhost -t 'icmis/#' -v
```

Confirm topic names, broker host, credentials, and that devices can route to the Pi.

### ONNX model fails to load

Check:

- ARM64-compatible `onnxruntime`;
- exact model path and file permissions;
- tensor layout/dimensions;
- class-map length/order;
- quantization scale and zero point;
- available RAM.

### No local LLM response

```bash
systemctl status ollama
ollama list
curl http://localhost:11434/api/tags
free -h
vcgencmd measure_temp
```

The system intentionally refuses local inference when RAM or thermal guardrails fail. Select `routingMode: "none"` for deterministic operation without an LLM.

### Camera or microphone unavailable

```bash
v4l2-ctl --list-devices
arecord -l
python -c "import sounddevice; print(sounddevice.query_devices())"
```

Check device permissions and add the deployment user to relevant groups if required:

```bash
sudo usermod -aG video,audio icmis
```

Log out and back in after changing groups.

### eDNA file is quarantined

Check required headers/metadata, scientific-name spelling, approved loci, coordinates, collection date, sequence alphabet, study bounds, and maximum allele count. Quarantining is intentional; do not bypass validation without examining the rejected record.

---

## Configuration reference

The following section explains every top-level `config.yaml` area and whether it normally needs deployment changes.

### `mqtt`

- `brokerHost`, `port`: broker connection.
- `topics.environmental`, `topics.telemetry`: subscription wildcards.
- Change when the broker or device topics differ.

### `storage`

- `databasePath`: primary SQLite/SpatiaLite database.
- `spatialiteExtension`: extension name/path.
- Review on every Pi.

### `database`

- Read pool, write queue, batching, busy timeout, and retry controls.
- `schemaFile` and `migrationsDirectory` should remain relative project paths.
- Leave `targetSchemaVersion` blank to apply all migrations.
- Threat radius/lookback are domain policy and should be scientifically reviewed.

### `filePaths`

- Camera traps, drone uploads, and quarantine folders.
- Change if mounted storage is used. Ensure the service user owns the locations.

### `validation` and `emaAlphas`

- Battery threshold and smoothing factors.
- Defaults are operational starting points. Calibrate to actual sensor behavior.

### `camera`

- Filesystem polling interval.
- Increase to reduce storage polling; decrease only if latency matters and CPU/storage can support it.

### `vision`

- Model path, tensor shape/layout, output parsing, thresholds, thread/queue controls, class labels, and quantization.
- Model metadata and class lists are deployment-required.

### `acoustic`

- Input directory, segmentation/spectrogram settings, live capture, model input, labels/thresholds, and device coordinates.
- Device/model values are deployment-required when enabled.

### `edna`

- Project, input/quarantine paths, spatial bounds/resolution, approved loci, and genetic thresholds.
- Study-specific values are required before using eDNA results.

### `aiEngine`

- State/model versions, CRI momentum, six domain weights, indicator/scaling rules, workflow limits, intervention policy, explainability limits, and LLM routing.
- Review scientifically before using scores for real decisions.
- Keep all six domain weights summing to `1.0`.

### `api`

- Server identity/network, CORS, prefix, dashboard, docs, database requirement, LLM warm-up, and routers.
- Keep debug off in production.
- Configure origins and authentication for the real deployment.
- Most endpoints shown in this README assume the default `/api/v1` prefix.

### `telemetryApi`

- Ingest queue, one worker, batching, validation, critical fast path, optional webhook/AI wake-up, pagination, SSE, and stats cache.
- Keep one ingest worker for SQLite unless architecture changes.
- Set `defaultSpeciesID` only with a defensible default.

### `analyticsApi`

- Caches, summary windows, rolling averages, metrics, downsampling, imputation, radar scoring, and clustering.
- Validate metrics/weights and keep requested point counts reasonable for a Pi.

### `reportsApi`

- In-process report job queue, one worker, timeout/retention, deduplication, spatial scope, tiers, pagination, and hydration.
- One worker is intentional for Pi memory safety.

### `websocketApi`

- Connection limits, allowed client types, send queue/timeout, heartbeat, H3 zones, priorities, CRI thresholds, telemetry forwarding, duplicate suppression, replay, and token policy.
- Enable token enforcement for real deployments.
- Mobile/web clients must maintain the heartbeat contract.

### `settingsApi`

- `stateDirectory` (shared with the root `icmis-apply` helper), `backupDirectory`, `maxBackups`, and the pre-flight switch and timeout.
- Leave `preflightEnabled: true`: it stops a bad value from reaching the running programs.

### `services`

- One `{enabled: true|false}` entry per continuous worker. Disabled workers are skipped at boot rather than failing.

### `schedules`

- `databaseBackup` (`enabled`, `onCalendar`, `keepCopies`, `directory`) and `riskAssessment` (`enabled`, `onCalendar`, `defaultLookbackDays`, `targets`); `onCalendar` uses systemd syntax such as `*-*-* 02:30:00`.
- `riskAssessment.targets` must list real species and locations (`{speciesID, scientificName, latitude, longitude}` or `{speciesID, h3Cell}`); if it is empty, nothing runs.

### `system`

- `hostname` (the Pi answers at `http://<hostname>.local`) and `timezone` (IANA name such as `Africa/Nairobi`). Both are applied to the OS by `deploy/applySettings.py`.

---

## Final commissioning checklist

- [ ] Pi OS, firmware, and packages are updated.
- [ ] Active cooling and stable storage/power are installed.
- [ ] Runtime directories exist and are owned by the deployment user.
- [ ] Python virtual environment and dependencies are installed on the Pi.
- [ ] SpatiaLite loads successfully.
- [ ] MQTT is secured appropriately and topic names match devices.
- [ ] Every mandatory deployment value above has been reviewed.
- [ ] Study bounds, loci, generation times, model labels, and device coordinates are real.
- [ ] Domain/risk thresholds have scientific approval.
- [ ] `.env` is mode `600`, excluded from source control, and contains no placeholder key.
- [ ] Cloud LLM use is disabled unless explicitly approved.
- [ ] Database migrations complete and a backup exists.
- [ ] Dashboard has been built and health reports `frontend_mounted: true`.
- [ ] Only required ingestion/inference workers are enabled.
- [ ] `sudo bash deploy/installServices.sh` has been run and the generated `ICMIS_API_KEY` is stored somewhere safe.
- [ ] After `sudo reboot`, `systemctl list-units 'icmis*' --all` shows the API and every ready worker `active`, with no manual action.
- [ ] `systemctl list-timers 'icmis*'` shows the backup and risk-assessment timers with the expected next run.
- [ ] The dashboard and the mobile app are paired under Pi Settings → Connection, and a test Save and Apply succeeds.
- [ ] `/api/v1/health`, MQTT ingestion, report generation, SSE, and WebSocket alert tests pass.
- [ ] The mobile app points to the Pi's reachable LAN address.
- [ ] Firewall/network exposure is limited to trusted clients.
- [ ] Backup, log review, update, and safe-shutdown procedures are understood.

For the raw command history used to assemble this deployment, see `setupCommands.txt`. This README is the preferred ordered procedure: `setupCommands.txt` includes useful module-specific development commands but is not intended to be pasted from top to bottom as one shell script.
