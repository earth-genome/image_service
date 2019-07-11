
## Image Service

This project establishes a web service for pulling satellite imagery from
various providers.

Extensive usage notes are given at top of the main modules in webapp/grabbers:
* Planet: planet_labs/planet_grabber.py
* DigitalGlobe: digital_globe/dg_grabber.py

Automated image mosaicking and color correction routines
are in the postprocessing subpackage to webapp/grabbers.

Manual mosaicking and color correction for images downloaded from
elsewhere is available in the manual_reprocessing package.

Routines to convert between vectors and rasters and to restore
georeferencing are in the georeferencing package.

### Dependencies 

Out-of-repo files .env and webapp/.google_config.json containing API
keys for Digital Globe, Planet, and Google cloud storage.

Our utilities repo as a submodule in webapp/grabbers. 

### Developing and deploying

The image service is based a containerized Flask web app, deployed on
Heroku. To test locally:

```bash
docker-compose build
docker-compose up
```

To deploy to Heroku, from the top-level directory:

```bash
heroku container:push --recursive -a earthrise-imagery
heroku container:release web worker thumbnailworker

```
