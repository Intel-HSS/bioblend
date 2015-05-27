#!/usr/bin/python
import galaxy_key;
from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.libraries import LibraryClient
#Create a file called galaxy_key and add your key there
import sys;
import uuid;
import csv;
import import_datasets_by_uuid;
import itertools;

#Exception class used by this module
class UploadFileException(Exception):
    def __init__(self, value):
        self.value = value;
    def __str__(self):
        return repr(self.value)

class UploadFileInfo(object):
    def __init__(self):
        #Fields required for INFO struct
        default_info_dict = { 'path':None, 'size':'4096', 'type':None, 'cluster':'', 'uuid':None, 'folder':'',
                'line_count':'1', 'skip':False};
        self.__dict__ = default_info_dict;

    def validate(self):
        try:
            uuid.UUID(self.uuid);
        except:
            self.uuid = str(uuid.uuid4())  #invalid UUID format, produce one
        if(not self.path):
            self.do_skip('Path missing for dataset');
        if(not self.type):
            self.do_skip('Type missing for dataset');

    def do_skip(self, message):
        sys.stdout.write('WARNING: '+message+', this dataset/UUID entry will be ignored\n');
        self.skip = True;

#Recursively create folder hierarchy within library
def get_and_create_hierarchy(library_client,library_id, parent_folder, parent_folder_id,
        folder_hierarchy_list, final_folder_id_list):
    if(len(folder_hierarchy_list) == 0):
        final_folder_id_list.append(parent_folder_id);
        return;
    next_folder = folder_hierarchy_list[0];
    new_folder_path = parent_folder+'/'+next_folder;
    if(len(library_client.get_folders(library_id, name=new_folder_path)) == 0):      #folder does not exist
        library_client.create_folder(library_id, next_folder, base_folder_id=parent_folder_id);
    for folder in library_client.get_folders(library_id, name=new_folder_path):
        folder_id = folder['id'];
        get_and_create_hierarchy(library_client, library_id, new_folder_path, folder_id,
                folder_hierarchy_list[1:], final_folder_id_list);

def upload_to_galaxy_library(galaxy_instance, library_client, library_name, upload_file_info_list):
    #Group by folders
    upload_file_info_list.sort(key=lambda info: info.folder);
    for library in galaxy_instance.libraries.get_libraries(name=library_name):     #iterate over list of dict
        library_id = library['id'];
        #For each upload folder collect list of datasets 
        for insert_folder_path, info_iter in itertools.groupby(upload_file_info_list,
                key=lambda info: info.folder):
            file_path_string = '';
            uuid_string = '';
            remote_dataset_type_string = '';
            file_size_string = '';
            line_count_string = '';
            first_iteration = True;
            for info in info_iter:
                if(info.skip):
                    continue;
                separator = '';
                if(not first_iteration):
                    separator = '\n';
                file_path_string += separator + info.path;
                uuid_string += separator + info.uuid;
                remote_dataset_type_string += separator + info.type;
                file_size_string += separator + info.size;
                line_count_string += separator + info.line_count;
                first_iteration = False;
            folder_id_list = [ None ];
            if(insert_folder_path != ''):
                tokens = insert_folder_path.split('/');
                path_tokens = [];
                for token in tokens:
                    if(token == ''):        #ignore empty strings
                        continue;
                    path_tokens.append(token);
                folder_id_list = [];
                get_and_create_hierarchy(library_client, library_id, '', None,
                        path_tokens, folder_id_list);
            for folder_id in folder_id_list:
                library_client.upload_from_galaxy_filesystem(library_id,
                    file_path_string,
                    link_data_only='link_to_files', remote_dataset = True, folder_id=folder_id,
                    uuid_list = uuid_string,
                    remote_dataset_type_list = remote_dataset_type_string,
                    file_size_list = file_size_string, 
                    line_count_list = line_count_string
                    );

if __name__ == "__main__":
    if(len(sys.argv) < 3):
        sys.stderr.write('Needs 2 arguments : <Galaxy_library_name> <csv_file>\n');
        sys.exit(-1);
    #Connect to Galaxy
    gi = GalaxyInstance(url=galaxy_key.galaxy_host, key=galaxy_key.galaxy_key);
    li = LibraryClient(gi);

    library_name = sys.argv[1];
    csv_file = sys.argv[2];
    try:
        fptr = open(csv_file, 'rb');
    except:
        sys.stderr.write('Could not open CSV file\n');
        sys.exit(-1);

    upload_file_info_list = [];
    #try:
        #dialect,id_field_name,fieldnames = import_datasets_by_uuid.check_and_return_header(fptr, delimiter=',');
        #csv_reader = csv.DictReader(fptr, fieldnames=fieldnames, dialect=dialect);
        #for row in csv_reader:
            #info = UploadFileInfo();
            #info.path = row.get('path', None);            #path
            #info.size = row.get('size', '4096');           #size in bytes
            #info.type = row.get('type', None);            #types - bam, sam, png etc
            #info.cluster = row.get('cluster', '');        #Optional: belongs to g1.spark0.intel.com or g2.spark0.intel.com (empty)
            #info.uuid = row.get(id_field_name, '');       #CCC_DID - optional (empty string)
            #info.folder = row.get('folder','');           #Optional: Folder name, empty (add to root directory of library)
            #upload_file_info_list.append(info);
    #except ImportDatasetsByUUIDException:
        #print('INFO: No header found, assuming old style CSV with fixed position fields');
    csv_reader = csv.reader(fptr, delimiter=',');
    for row_tokens in csv_reader:
        if(len(row_tokens) > 0):
            if(row_tokens[0][0] == '#'):
                continue;
            assert len(row_tokens) >= 4;
            info = UploadFileInfo();
            info.path = row_tokens[0];      #path
            info.size = row_tokens[1];      #size in bytes
            info.type = row_tokens[2];      #types - bam, sam, png etc
            info.cluster = row_tokens[3];         #belongs to g1.spark0.intel.com or g2.spark0.intel.com
            if(len(row_tokens) >= 5):
                info.uuid = row_tokens[4];        #CCC_DID - optional
            if(len(row_tokens) >= 6):
                info.folder = row_tokens[5];        #folder name - optional
            upload_file_info_list.append(info);
    for info in upload_file_info_list:
        info.validate();
    upload_to_galaxy_library(gi, li, library_name, upload_file_info_list);

