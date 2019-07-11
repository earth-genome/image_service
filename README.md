<<<<<<< HEAD
## Image Service

This project establishes a web service for pulling satellite imagery from
various providers. 

### Dependencies 

Code from the grab_imagery repo is invoked to process the pull requests. It is contained as a submodule.

Out-of-repo files .env and webapp/.google_config.json containing API keys for Digital Globe, Planet, and Google cloud storage.

### Developing

The image service is based a containerized Flask web app, deployed on
Heroku.  To test the app locally, simply use the following command from the top-level
directory:

```bash
docker-compose up
```

If you change the environment (e.g., adding a dependency), then you will first
have to rebuild the container with:

```bash
docker-compose build
```

### Deploying

The app name on Heroku for this project is `earthrise-imagery`.  As such,
when deploying live, simply use the following command from the top-level
directory.

```bash
heroku container:push --recursive -a earthrise-imagery
heroku container:release web worker thumbnailworker

```
=======
# grab-imagery
Tools to grab imagery directly from providers. 

Extensive usage notes are given at top of the main modules:
* Planet: planet_labs/planet_grabber.py
* DigitalGlobe: digital_globe/dg_grabber.py
* All providers, with interface to Google cloud storage: grabber_handlers.py

Image compositing, mosaicking, and color correction routines are in postprocessing (automated) and manual_processing folders.

Requires out-of-repo API keys for Digital Globe, Planet, and Google cloud storage stored as environment variables.
>>>>>>> grab_imagery-origin/master
