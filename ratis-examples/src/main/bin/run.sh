BIN=ratis-examples/src/main/bin;
PEERS=n0:127.0.0.1:6000,n1:127.0.0.1:6001,n2:127.0.0.1:6002;

ID=n0; sh server.sh arithmetic server --id ${ID} --storage /tmp/ratis/${ID} --peers ${PEERS};
