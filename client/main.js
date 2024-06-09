


const client = proto.client
const { ClientServiceClient } = client;
const {
    ClientPingRequest, ClientGetRequest, ClientSetRequest,
    ClientStrlnRequest, ClientDelRequest, ClientAppendRequest,
    ClientRequestLogRequest
} = client;

var PORT = document.getElementById('port').value;

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
                const leaderPort = parseInt(response.getLeaderport()) + 1;
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
                  const leaderPort = parseInt(response.getLeaderport()) + 1;
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
                    const leaderPort = parseInt(response.getLeaderport()) + 1;
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
                    const leaderPort = parseInt(response.getLeaderport()) + 1;
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
                    const leaderPort = parseInt(response.getLeaderport()) + 1;
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
                    const leaderPort = parseInt(response.getLeaderport()) + 1;
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
        } else {
            addOutput('Invalid command', true);
        }
  }
});

