import groovyx.net.http.RESTClient

def bswtFlagApi = new RESTClient(args[0])

// TODO maybe do logging here instead?
println "setting flag '${args[1]}' to '${args[2]}'"
def resp = bswtFlagApi = bswtFlagApi.put(path: "${args[1]}/${args[2]}")

assert resp.status == 200