"""Phase D: a Danish fine-tune on frozen BirdNET 2.4 embeddings.

Phases B and C asked whether a spatiotemporal occurrence prior improves a strong
classifier; on Danish recordings (DKX) it does not. Phase D instead trains a
small classifier head on BirdNET's own 1024-d embeddings of European
Xeno-canto recordings (BirdSet XCL inside a European box) and fuses it with
BirdNET's scores:

    fused = log s_birdnet + alpha * log q_head

Modules
  taxonomy  XCL/Xeno-canto label codes -> BirdNET's label axis (renamed taxa)
  head      the head: training, scoring, fusion, save/load (needs torch)
  noise     bird-free urban/outdoor noise mixed into recordings at fixed SNRs
  evaluate  top-1 / recordist-clustered bootstrap CIs on per-recording scores
"""
