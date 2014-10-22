import urllib2
import simplejson

_id_list = None


def get_youtube_ids():
    """Retrieve all the YouTube IDs (including translations) from KA.org.
    
    This will fetch all the publicly-visible YouTube IDs on the site in all
    languages and return them as a big, flat list.
    """
    global _id_list
    if _id_list is None:
        all_videos_in = urllib2.urlopen("http://www.khanacademy.org/api/internal/videos/localized/all")
        try:
            all_videos = simplejson.load(all_videos_in)
        finally:
            all_videos_in.close()

        # Now get our CS videos that are not part of the content topic tree,
        # but are part of the scratchpad tutorials
        all_talkies_in = urllib2.urlopen(
            "https://www.khanacademy.org/api/labs/scratchpads/tutorial?verbose=false")
        try:
            all_talkies = simplejson.load(all_talkies_in)
        finally:
            all_talkies_in.close()

        _id_list = []
        for v in all_videos:
            _id_list += v["youtube_ids"].values()

        for s in all_talkies:
            youtube_id = s["revision"].get("youtube_id")
            if youtube_id:
                _id_list.append(youtube_id)
    
    return _id_list

