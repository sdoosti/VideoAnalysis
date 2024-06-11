import os
import argparse

def main():
    parser = argparse.ArgumentParser(description='Generate a list of files in a directory')
    parser.add_argument('-d','--directory', help='The directory to list')
    parser.add_argument('-o','--output', help='The file to write the list to', default='files.txt')
    args = parser.parse_args()

    video_files = []
    for root, dirs, files in os.walk(args.directory):
        for file in files:
            #print(os.path.join(root, file))
            if file.endswith('.mp4') or file.endswith('.mkv') or file.endswith('.avi'):
                video_files.append(os.path.join(root, file))

    print('Found {} video files'.format(len(video_files)))
    # print sample of files
    for i in range(5):
        print(video_files[i])
        
    with open(args.output, 'w') as f:
        for video_file in video_files:
            f.write(video_file + '\n')