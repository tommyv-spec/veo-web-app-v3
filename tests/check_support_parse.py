import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import image_platform as ip

MD = '''
## Images

### Image 1
- **Image prompt:**
```
Use the uploaded character reference image for the main character. Talking head. Aspect ratio 9:16.
```

### Image 7
- **Image prompt:**
```
A pile of turmeric root and powder on a wooden board. Aspect ratio 9:16.
```

## Storyboard

### Scene 1
- **image:** image_1
- **line:** the number one anti-inflammatory compound in the world is not turmeric

## Support Inserts

### Support 1
- **image:** image_7
- **start_word:** not
- **end_word:** turmeric
- **phrase:** not turmeric
'''

parsed = ip.parse_scene_table(MD)
si = parsed["support_inserts"]
assert len(si) == 1, si
s = si[0]
assert s["support_index"] == 1
assert s["image_index"] == 7
assert s["start_word"] == "not"
assert s["end_word"] == "turmeric"
assert s["phrase"] == "not turmeric"
assert len(parsed["images"]) == 2
assert len(parsed["scenes"]) == 1
print("OK support parse")
