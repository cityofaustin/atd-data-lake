# Appendix: Time Zone

*[(Back to Docs Catalog)](index.md)*

This document captures notes on setting the time zone of the running EC2 instance that hosts PostgREST and the ETL tasks.

## Time Zone Set Procedure
Verify Linux distribution:

```bash
cat /etc/*release
```

Review time and date settings:

```bash
timedatectl
```

List the valid time zones:

```bash
timedatectl list-timezones | grep Chicago
```

Set the time zone:

```bash
sudo timedatectl set-timezone "America/Chicago"
```

Reboot the server

```bash
sudo shutdown -r now
```

Log back in when the system is up.

## Restarting Services:
Eventually this should be automated. But for the time being, services must be restarted manually. First, start PostgREST. Run from the home directory (`/home/ec2-user`):

```bash
sudo systemctl start docker
sudo `which docker-compose` up -d 
```

That refers to the `docker-compose.yml` file, which contains:

```yml
server:
  image: postgrest/postgrest
  ports:
    - "80:3000"
  environment:
    PGRST_DB_URI: postgres://atduser:***@atd-rodeo-test-***.rds.amazonaws.com/atd01
    PGRST_DB_SCHEMA: api
    PGRST_DB_ANON_ROLE: web_anon
    PGRST_JWT_SECRET: ***
    PGRST_MAX_ROWS: 5000
```

We also need to re-enable the swap space (until we actually get to adding it to `/etc/fstab`):

```bash
sudo swapon /swapfile
```
