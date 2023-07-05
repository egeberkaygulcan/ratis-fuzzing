while getopts i:r: flag
do
    case "${flag}" in
        i) id=${OPTARG};;
        a) random=${OPTARG};;
    esac
done

BIN=src/main/bin;
PEERS=n0:127.0.0.1:6000,n1:127.0.0.1:6001,n2:127.0.0.1:6002;
ID=$id; 
RANDOM=$random;
${BIN}/server.sh arithmetic server --id ${ID} --storage /tmp/ratis/${ID} --peers ${PEERS} --random-exec ${RANDOM}