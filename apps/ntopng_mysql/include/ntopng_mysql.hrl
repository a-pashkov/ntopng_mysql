-record(packet, {
  src_addr, 
  src_port, 
  dst_addr, 
  dst_port, 
  in_bytes, 
  out_bytes, 
  in_pkts, 
  out_pkts, 
  last_switched, 
  turn=false}).

