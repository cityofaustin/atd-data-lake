# Testing Setup

A new series of additions are needed to adapt the processing framework for offline testing. This involves generalizing the catalog to a SQLAlchemy driver, and also generalizing other devices to local files that abide by a naming convention. These not only allow for offline testing of ETL process, but they also open the door for easing future development without the need to set up cloud-based dependencies.

We need to figure out:

* Whether to have a new testing mode that specifically utilizes these classes, or if they operate as a baseline debug that's enabled through a global command-line flag (+ options)

Thing to do:

- [ ] Consider a flag that enables default debug/test. Factories create/access things that work in a test data directory structure and support SQLite (via SQLAlchemy) for the Catalog. 
- [ ] Implement classes that allow for direct file access, Catalog access, etc.
- [ ] What about something that queries for the presence of a catalog entry (given Purpose, debug flag, etc.)?
- [ ] Anything else that can help with items that update more frequently than once per day?
