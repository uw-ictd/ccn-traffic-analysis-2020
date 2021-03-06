from collections import namedtuple


TypicalFlow = namedtuple("TypicalFlow", [
    "start",
    "end",
    "user",
    "dest_ip",
    "user_port",
    "dest_port",
    "bytes_up",
    "bytes_down",
    "protocol",
])

AnomalyPeerToPeerFlow = namedtuple("AnomalyPeerToPeerFlow", [
    "start",
    "end",
    "user_a",
    "user_b",
    "a_port",
    "b_port",
    "bytes_a_to_b",
    "bytes_b_to_a",
    "protocol",
])

AnomalyNoUserFlow = namedtuple("AnomalyNoUserFlow", [
    "start",
    "end",
    "ip_a",
    "ip_b",
    "a_port",
    "b_port",
    "bytes_a_to_b",
    "bytes_b_to_a",
    "protocol",
])

DnsResponse = namedtuple("DnsResponse", [
    "timestamp",
    "user",
    "dns_server",
    "user_port",
    "server_port",
    "protocol",
    "opcode",
    "resultcode",
    "domain_name",
    "ip_address",
    "ttl",
])

Transaction = namedtuple("Transaction", [
    "timestamp",
    "kind",
    "user",
    "dest_user",
    "amount_bytes",
    "amount_idr",
])
