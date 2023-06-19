# Testing Setup

A new series of additions are needed to adapt the processing framework for offline testing. This involves generalizing the catalog to a SQLAlchemy driver, and also generalizing other devices to local files that abide by a naming convention. These not only allow for offline testing of ETL process, but they also open the door for easing future development without the need to set up cloud-based dependencies.

We need to figure out:

* Whether to have a new testing mode that specifically utilizes these classes, or if they operate as a baseline debug that's enabled through a global command-line

Thing to do:

- [ ] The
- [ ] Tho
