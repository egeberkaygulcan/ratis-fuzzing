BIN=src/main/bin;
PEERS=n0:127.0.0.1:6000,n1:127.0.0.1:6001,n2:127.0.0.1:6002;

${BIN}/client.sh arithmetic assign --name a --value 3 --peers ${PEERS};
${BIN}/client.sh arithmetic assign --name b --value 4 --peers ${PEERS};
${BIN}/client.sh arithmetic assign --name c --value a+b --peers ${PEERS};
${BIN}/client.sh arithmetic get --name c --peers ${PEERS};