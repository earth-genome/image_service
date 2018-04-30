## Newswire

This project creates the front-end view of the Earthrise newswire.

### Developing

The newswire is based a containerized Flask and Jinja web app, deployed on
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

The app name on Heroku for this project is `earthrise-newswire`.  As such,
when deploying live, simply use the following command from the top-level
directory.

```bash
heroku container:push web -a earthrise-newswire
```