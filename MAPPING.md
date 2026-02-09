# Video File Mapping

## Season-based filename mapping

The `parse-video-filename` function maps Internet Archive filenames to episodes via the `season-base` offset system:

| Season | Filename range | Episodes covered |
|--------|---------------|-----------------|
| 1 | IC1{01-10} | 1-10 |
| 2 | IC2{01-50} | 11-60 |
| 3 | IC3{01-50} | 61-110 |
| 4 | IC4{01-52} | 111-162 |
| 5 | IC5{01-48} | 163-210 |
| 6 | IC6{01-81} | 211-291 |

## Special-case filenames

| Pattern | Episode(s) | Notes |
|---------|-----------|-------|
| 97ICWC* | 198 | 1997 World Cup |
| IC415-416 | 125, 126 | France Special (two episodes) |
| IC452OA-NYE* | 162 | New Year's Eve special |
| IC315OA85* | 75 | Ignores "85" suffix |

## Unlinked episodes

Episodes 292-295 (2000-2002 specials) have no filename mapping in `parse-video-filename`:

- **292** - Millennium Cup (2000)
- **293** - New York Special (2001)
- **294** - 21st Century Battles (2001)
- **295** - Japan Cup (2002)

If these specials exist on the Internet Archive under some naming convention, they will need additional special-case handlers added to `parse-video-filename`.
