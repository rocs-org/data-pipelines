Welcome to ROCS dbt setup! Here we keep all dbt models for all projects :)


### Setup
To develop against our main database you need to do two things
- set up dbt, either in a Python env (there is a poetry.lock in this directory), or via an option in https://docs.getdbt.com/dbt-cli/install/overview
- create / modify the profiles.yml at ~/.dbt/profiles.yml to contain:

```
data-pipelines: # this is the project name
  outputs:
    dev:
      type: postgres
      threads: 4
      host: host #from your db .cfg file
      port: port #from your db .cfg file
      user: username #from your db .cfg file
      pass: password #from your db .cfg file
      dbname: dbname #from your db .cfg file
      schema: your_name #this should be the name of your personal playground schema, usually the same as username, this is where the resulting tables of your dbt models will arrive

  target: dev
  ```
  Now you will be able work to work directly in the dbt directory here in the `data-pipelines` repository

### User guide

- each project has its own folder in /models
- to add a model it is enough to create a new .sql file, use existing ones for reference
- each folder contains a schema.yml with metadata for the .sql models
- after developing and testing it works (we are still figuring out efficient testing) you can open a PR
- You can ask Jakob and David for help!


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
