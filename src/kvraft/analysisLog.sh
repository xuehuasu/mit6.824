#!/bin/bash

op=""
word=""
inputfile="linshi.log"
num=""

while getopts "f:w:b:n" opt; do
  case $opt in
    n)
      op=$op" -n"
      ;;
    f)
      inputfile=$OPTARG
      ;;
	w)
	  word=$OPTARG
	  ;;
	b)
	  num=$OPTARG
	  ;;
    \?)
      echo "Invalid option: -$OPTARG"
      ;;
  esac
done

commands=("\"server PutAppend result:.*${word}\"" 
          "\"server applier.*${word}\""
          "\"commitLog.*${word}\""
          "\"server PutAppend:.*${word}\""
          "\"server Get result:.*${word}\""
          "\"client\""
          "\"server PutAppend.*${word}\""
		  "\"leader is\""
          "\"raft ticker lock:.*\""
          "\"appendLog.*\""
          "\"CondInstallSnapshot.*\""
          "\"AppendEntry lock.*\""
          "\"sendMsg:.*\""
          "-E \"exit:|start \""
          "\"sendSnapshot.*\""
          "\" Snapshot lock:.\""
          "\"dosnapshot.*\""
          "\"persist:\""
          "\"RPC InstallSnapshot lock\""
          "\"sendMsg lock: \""
          "\"sendVote\""
          "\"ticker\""
		  )
echo 'begin' > loginfo.txt
for i in "${!commands[@]}"; do
    cmd="${commands[$i]}"
    outputfile="$i".log
   	eval "grep ${cmd} ${inputfile}" > "${outputfile}${num}" "${op}"
    echo "${cmd}: ${outputfile}"
    
    echo "${cmd}: ${outputfile}" >> loginfo.txt
done
