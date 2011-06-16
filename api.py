
import urllib2
import simplejson

def list_new_videos():

    file = urllib2.urlopen("http://www.khanacademy.org/api/v1/playlists/library/list")

    try:
        library = simplejson.loads(file.read())
    except:
        # TODO: Error logging here
        pass
    finally:
        file.close()

    videos_new = []

    for playlist in library:
        for video in playlist["videos"]:
            if not video["download_url"]:
                videos_new.append(video)

    return videos_new
