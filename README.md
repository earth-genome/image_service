
## Image Service

This project establishes a web service for pulling satellite imagery from
various providers. Links to available services and detailed instructions are given 
at the primary entry point[http://earthrise-imagery.herokuapp.com].

There are also several tools available for local image processing: 
* Manual_reprocessing: mosaicking and color correction for image tiles downloaded elsewhere.
* Georeferencing: Conversions between vectors and rasters and a tool to restore georeferencing to Photoshopped images.

Access to the satellite provider APIs is also available locally. See use notes in the modules in webapp/grabbers.

### Dependencies 

* API keys in out-of-repo files .env and webapp/.google_config.json.
* Our utilities repo as a submodule in webapp/grabbers. 

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
