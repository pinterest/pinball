convert \
 <(convert favicon.png -filter LanczosSharp -resize 16x16 png:-) \
 <(convert favicon.png -filter LanczosSharp -resize 32x32 png:-) \
 <(convert favicon.png -filter LanczosSharp -resize 64x64 png:-) \
 <(convert favicon.png -filter LanczosSharp -resize 128x128 png:-) \
 favicon.png favicon.ico
