const { RaftStateRequest } = require("./generated/proto/raft_grpc_web_pb");



const client = proto.client
const { ClientServiceClient } = client;
const {
    ClientPingRequest, ClientGetRequest, ClientSetRequest,
    ClientStrlnRequest, ClientDelRequest, ClientAppendRequest
} = client;

var PORT = document.getElementById('port').value;
PORT = 7000

var clientService = new proto.client.ClientServiceClient(`http://localhost:${PORT}`, null, null);
document.getElementById('port').addEventListener('keypress', (event) => {
    if (event.key === 'Enter') {
        const port = event.target.value;
        clientService = new proto.client.ClientServiceClient('http://localhost:' + port, null, null);
        addOutput('=/ Port changed to ' + port);
    }
});


const output = document.getElementById('output');

function addOutput(str, err=false) {
    if (err) {
        output.innerHTML += '<a style="color: #ad0000;">' + str + '</a><br>';
    } else {
    output.innerHTML += str + '<br>';
    }
  output.scrollTop = output.scrollHeight;
}

document.getElementById('command').addEventListener('keypress', (event) => {
    if (event.key === 'Enter') {
        const command = event.target.value;
        addOutput('$ ' + command);
        event.target.value = '';
        const tokens = command.split(' ');
        if (tokens[0] == 'ping') {
            const request = new ClientPingRequest();
            request.setMessage(tokens[1]);
  
            clientService.ping(request, {}, (err, response) => {
                if (err) {
                      addOutput('Error: ' + err.message, true);
                      console.error(err);
                } else {
                    addOutput(response.getMessage());
                    console.log(response.getMessage());
                }
            });
        } else if (tokens[0] == 'get') {
            const request = new ClientGetRequest();
            request.setKey(tokens[1]);
  
            clientService.get(request, {}, (err, response) => {
                if (err) {
                      addOutput('Error: ' + err.message, true);
                    console.error(err);
                } else {
                  const leaderPort = parseInt(response.getLeaderport()) - 1000;
                  if (!response.getSuccess() && leaderPort) {
                      const tmp = new proto.client.ClientServiceClient('http://localhost:' + leaderPort, null, null);
                      addOutput(`Redirect: leader at localhost:${leaderPort}`, true)
                      tmp.get(request, {}, (err, response) => {
                          if (err) {
                              addOutput('Error: ' + err.message, true);
                              console.error(err);
                          } else {
                              addOutput(response.getValue() ?? "Key not found");
                              console.log(response.getValue());
                          }
                      });
                  } else {
                      if (response.getSuccess()) {
                          addOutput(response.getValue());
                  } else {
                      addOutput('Key not found', true);
                  }
                  }
  
                }
            });
        } else if (tokens[0] == 'set') {
            const request = new ClientSetRequest();
            request.setKey(tokens[1]);
            request.setValue(tokens[2]);
  
            clientService.set(request, {}, (err, response) => {
                if (err) {
                      addOutput('Error: ' + err.message, true);
                    console.error(err);
                } else {
                    const leaderPort = parseInt(response.getLeaderport()) - 1000;
                      if (!response.getSuccess() && leaderPort) {
                          const tmp = new proto.client.ClientServiceClient('http://localhost:' + leaderPort, null, null);
                          addOutput(`Redirect: leader at localhost:${leaderPort}`, true)
                          tmp.set(request, {}, (err, response) => {
                              if (err) {
                                  addOutput('Error: ' + err.message, true);
                                  console.error(err);
                              } else {
                                  addOutput("Set success" ?? "Key not found");
                                  console.log(response.getValue());
                              }
                          });
                      } else {
                          if (response.getSuccess()) {
                              addOutput('Set success');
                          } else {
                              addOutput('Set failed', true);
                          }
                      }
                }
            });
          } else if (tokens[0] == 'strln') {
              const request = new ClientStrlnRequest();
              request.setKey(tokens[1]);
  
              clientService.strln(request, {}, (err, response) => {
                  if (err) {
                      addOutput('Error: ' + err.message, true);
                      console.error(err);
                  } else {
                      const leaderPort = parseInt(response.getLeaderport()) - 1000;
                      if (!response.getSuccess() && leaderPort) {
                          const tmp = new proto.client.ClientServiceClient('http://localhost:' + leaderPort, null, null);
                          addOutput(`Redirect: leader at localhost:${leaderPort}`, true)
                          tmp.strln(request, {}, (err, response) => {
                              if (err) {
                                  addOutput('Error: ' + err.message, true);
                                  console.error(err);
                              } else {
                                  addOutput(response.array[0] ?? "Key not found");
                                  console.log("HOHO:", response.getValue());
                              }
                          });
                      } else {
                          if (response.getSuccess()) {
                              addOutput(response.array[0]);
                          } else {
                              addOutput('Key not found', true);
                          }
                      }
                  }
              });
          } else if (tokens[0] == 'del') {
              const request = new ClientDelRequest();
              request.setKey(tokens[1]);
  
              clientService.del(request, {}, (err, response) => {
                  if (err) {
                      addOutput('Error: ' + err.message, true);
                      console.error(err);
                  } else {
                      const leaderPort = parseInt(response.getLeaderport()) - 1000;
                      if (!response.getSuccess() && leaderPort) {
                          const tmp = new proto.client.ClientServiceClient('http://localhost:' + leaderPort, null, null);
                          addOutput(`Redirect: leader at localhost:${leaderPort}`, true)
                          tmp.del(request, {}, (err, response) => {
                              if (err) {
                                  addOutput('Error: ' + err.message, true);
                                  console.error(err);
                              } else {
                                  addOutput("Delete success" ?? "Key not found");
                                  console.log(response.getValue());
                              }
                          });
                      } else {
                          if (response.getSuccess()) {
                              addOutput('Delete success');
                          } else {
                              addOutput('Key not found', true);
                          }
                      }
                  }
              });
          } else if (tokens[0] == 'append') {
              const request = new ClientAppendRequest();
              request.setKey(tokens[1]);
              request.setValue(tokens[2]);
  
              clientService.append(request, {}, (err, response) => {
                  if (err) {
                      addOutput('Error: ' + err.message, true);
                      console.error(err);
                  } else {
                      const leaderPort = parseInt(response.getLeaderport()) - 1000;
                      if (!response.getSuccess() && leaderPort) {
                          const tmp = new proto.client.ClientServiceClient('http://localhost:' + leaderPort, null, null);
                          addOutput(`Redirect: leader at localhost:${leaderPort}`, true)
                          tmp.append(request, {}, (err, response) => {
                              if (err) {
                                  addOutput('Error: ' + err.message, true);
                                  console.error(err);
                              } else {
                                  addOutput("Append success. Result: " + response.getValue()  ?? "Key not found");
                                  console.log(response.getValue());
                              }
                          });
                      } else {
                          if (response.getSuccess()) {
                              addOutput("Append success. Result: " + response.getValue());
                          } else {
                              addOutput('Append failed', true);
                          }
                      }
                  }
              });
            } else if (tokens[0] == 'reqlog') {
                const request = new ClientRequestLogRequest();
                clientService.reqLog(request, {}, (err, response) => {
                    if (err) {
                        addOutput('Error: ' + err.message, true);
                        console.error(err);
                    } else {
                        const leaderPort = parseInt(response.getLeaderport()) - 1000;
                        if (!response.getSuccess() && leaderPort) {
                            const tmp = new proto.client.ClientServiceClient('http://localhost:' + leaderPort, null, null);
                            addOutput(`Redirect: leader at localhost:${leaderPort}`, true)
                            tmp.reqLog(request, {}, (err, response) => {
                                if (err) {
                                    addOutput('Error: ' + err.message, true);
                                    console.error(err);
                                } else {
                                    addOutput(response.getValue());
                                    console.log(response.getValue());
                                }
                            });
                        } else {
                            if (response.getSuccess()) {
                                addOutput(response.getValue());
                            } else {
                                addOutput('Request log failed', true);
                            }
                        }
                    }
                });
    
  
        } else if (tokens[0] == 'show') {
            const skibidi = document.getElementById(tokens[1]);
            if (!skibidi) {
                const status = document.getElementById('status');
                status.innerHTML += `<div id="${tokens[1]}" class="element" ">`
                updateRequest(tokens[1]);
            }
        } else {
            addOutput('Invalid command', true);
        } 
  }
});

function updateRequest(port) {
    setInterval(() => {
        sendRaftStateRequest(port);
    }, 1000);
}

function sendRaftStateRequest(port) {
    const request = new RaftStateRequest();
    const raftService = new proto.raft.RaftServiceClient('http://localhost:' + port, null, null);

    raftService.state(request, {}, (err, response) => {
        const div = document.getElementById(port);
        if (err) {
            div.innerHTML = `State port ${port}<br>`
            div.innerHTML += 'DIED';
        } else {
            div.innerHTML = `State port ${port}<br>`
            const state = JSON.stringify(JSON.parse(response.array[0][0]), undefined, 2);
            const statemachine = JSON.stringify(JSON.parse(response.array[0][2]), undefined, 2);
            const log = JSON.stringify(JSON.parse(response.array[0][1]), undefined, 2); 
            div.innerHTML += "<pre>" + state + "</pre>";
            div.innerHTML += "State Machine<br>";   
            div.innerHTML += "<pre>" + statemachine + "</pre>";
            div.innerHTML += "Log<br>";
            div.innerHTML += "<pre>" + log + "</pre>";
            div.innerHTML += response.array[0][3]; 

        }
    });
}



