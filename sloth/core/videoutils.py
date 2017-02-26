import cv2
from datetime import datetime
import json
import subprocess
import tempfile

def _parse_time_ms(time_str):
    m_epoch = datetime.strptime('0', '%S')
    num_colon = time_str.count(':')
    if num_colon == 0:
        m_time = datetime.strptime(time_str, '%S')
    elif num_colon == 1:
        m_time = datetime.strptime(time_str, '%M:%S')
    elif num_colon == 2:
        m_time = datetime.strptime(time_str, '%H:%M:%S')
    else:
        raise ValueError('Invalid time string: "{}"'.format(time_str))
    return (m_time - m_epoch).seconds * 1000

class VideoObject(object):
    def __init__(self, filename, metadata, sample_rate=0.1):
        self.filename = filename
        self.video = cv2.VideoCapture(filename)
        self.metadata = metadata
        if 'start' in metadata and metadata['start'] != 'start':
            self.start_ms = _parse_time_ms(metadata['start'])
        else:
            self.start_ms = 0

        if 'end' in metadata and metadata['end'] != 'end':
            self.end_ms = _parse_time_ms(metadata['end'])
        else:
            self.video.set(cv2.CAP_PROP_POS_FRAMES, self.video.get(cv2.CAP_PROP_FRAME_COUNT))
            self.end_ms = self.video.get(cv2.CAP_PROP_POS_MSEC)

        if 'sample_rate' in metadata:
            self.sample_rate = metadata['sample_rate']
        else:
            self.sample_rate = sample_rate

        self.nframes = None

    def get_nframes(self):
        if self.nframes is None:
            self.video.set(cv2.CAP_PROP_POS_MSEC, self.start_ms)
            sframe = self.video.get(cv2.CAP_PROP_POS_FRAMES)
            self.video.set(cv2.CAP_PROP_POS_MSEC, self.end_ms)
            eframe = self.video.get(cv2.CAP_PROP_POS_FRAMES)
            self.nframes = int(eframe - sframe)
        return self.nframes

    def gen_timestamps(self):
        t = self.start_ms
        self.video.set(cv2.CAP_PROP_POS_MSEC, self.start_ms)
        i = int(self.video.get(cv2.CAP_PROP_POS_FRAMES))
        fps = self.video.get(cv2.CAP_PROP_FPS)

        while self.video.get(cv2.CAP_PROP_POS_MSEC) < self.end_ms:
            yield (self.video.get(cv2.CAP_PROP_POS_FRAMES), self.video.get(cv2.CAP_PROP_POS_MSEC)/1000)
            self.video.grab()

    def get_frame(self, frameno):
        if self.video.get(cv2.CAP_PROP_POS_FRAMES) != frameno:
            self.video.set(cv2.CAP_PROP_POS_FRAMES, frameno)
        return self.video.read()[1]

class RemoteVideo(VideoObject):
    def __init__(self, metadata):
        self.tempfile = tempfile.NamedTemporaryFile()
        self.remote = metadata['location']
        self.path = metadata['path']
        temp_filename = self.tempfile.name
        subprocess.check_call(['sshfs', '{}:{}'.format(self.remote, self.path), temp_filename])
        # this must be last, because VideoObject assumes that filename is readable as a video file
        super(RemoteVideo, self).__init__(temp_filename, metadata)

    def __del__(self):
        self.video.release()
        subprocess.call(['fusermount', '-u', self.filename])

def load_video(filename):
    if filename.endswith('json'):
        return load_video_from_metadata(filename)
    else:
        return load_video_from_video_file(filename)


def load_video_from_metadata(metadata_fname):
    with open(metadata_fname) as metadata_f:
        metadata = json.load(metadata_f)
        if 'location' in metadata and metadata['location'] != 'local':
            return RemoteVideo(metadata)
        else:
            path = metadata['path']
            if not os.path.isabs(path):
                path = os.path.join(os.path.dirname(metadata_fname, path))
            return VideoObject(path, metadata)

def load_video_from_video_file(fname):
    return VideoObject(fname, {'path': fname}) 

