import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import image_worker as iw
m = iw._IMG_API_ASPECT_MAP
assert m["4:3"] == "IMAGE_ASPECT_RATIO_LANDSCAPE_FOUR_THREE"
assert m["3:4"] == "IMAGE_ASPECT_RATIO_PORTRAIT_THREE_FOUR"
assert m["9:16"] == "IMAGE_ASPECT_RATIO_PORTRAIT"
print("OK aspect enum map")
