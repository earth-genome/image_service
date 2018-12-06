# grab-imagery
Tools to grab imagery directly from providers. 

Extensive usage notes are given at top of the main modules:
* Planet: planet_labs/planet_grabber.py
* DigitalGlobe: digital_globe/dg_grabber.py
* All providers, with interface to Google cloud storage: grabber_handlers.py

Image compositing, mosaicking, and color correction routines are in postprocessing (automated) and manual_processing folders.

Requires out-of-repo API keys for Digital Globe, Planet, and Google cloud storage stored as environment variables.
