using Workerd = import "/workerd/workerd.capnp";

const helloWorldExample :Workerd.Config = (
  services = [ (name = "main", worker = .helloWorld) ],
  sockets = [ ( name = "http", address = "*:0", http = (), services = ["main"] ) ],
);

const helloWorld :Workerd.Worker = (
  serviceWorkerScript = embed "worker.js",
  compatibilityDate = "2023-02-28",
);
