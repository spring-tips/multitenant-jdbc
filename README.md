# Multitenant JDBC 

You'll need to spin up two separate PostgreSQL instances. Put  this script into a file called `postgres.sh`:

```shell
#!/usr/bin/env bash
NAME=${1:-default}-postgres
PORT=${2:-5432}
docker run -d  --name  $NAME  \
    -p ${PORT}:5432 \
    -e POSTGRES_USER=user \
    -e PGUSER=user \
    -e POSTGRES_PASSWORD=pw \
postgres:latest
```

Don't forget to make it executable if you're on a UNIX flavor OS: 

```shell 
chmod a+x ./postgres.sh
```

Then, run it twice to spin up two different Docker images:

```shell 
./postgres.sh pg1 5431 
./postgres.sh pg2 5432
```

Then, you can run the application. You should be able to login and observe that there are different datasets in each one. 

You can login to each instance like this: 

```shell 
PGPASSWORD=pw psql -U user -h localhost -p 5431 user 
```