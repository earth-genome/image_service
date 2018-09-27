## Image Service

This project establishes a web service for pulling satellite imagery from
various providers.

### Developing

The image service is based a containerized Flask web app, deployed on
Heroku.  To test the app, simply use the following command from the top-level
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
