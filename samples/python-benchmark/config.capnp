using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "main", worker = .mainWorker),
  ],
  sockets = [ ( name = "http", address = "*:8080", http = (), services = ["main"] ) ],
);

const mainWorker :Workerd.Worker = (
  modules = [
    (name = "worker.py", pythonModule = embed "./worker.py"),
  ],
  compatibilityDate = "2025-11-15",
  compatibilityFlags = ["python_workers"],
);
