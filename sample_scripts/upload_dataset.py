#!/usr/bin/python
import galaxy_key;
from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.libraries import LibraryClient
#Create a file called galaxy_key and add your key there
import sys;
import uuid;
import csv;

def upload_to_galaxy_library(galaxy_instance, library_client, library_name, file_paths_list, file_sizes_list, file_types_list,
        cluster_list=None, uuid_list=None):
    if(uuid_list == None or len(uuid_list) == 0):
        uuid_list = [];
        for file in file_paths_list:
          uuid_list.append(str(uuid.uuid4()));

    if(len(file_sizes_list) != len(file_paths_list) or len(file_types_list) != len(file_paths_list) or len(file_paths_list) != len(uuid_list)
            or len(cluster_list) != len(file_paths_list)):
      sys.stderr.write('File names, file sizes, file types, cluster and UUID lists lengths should be the same\n');
      sys.exit(-1);

    idx = 0;
    for file in file_paths_list:
        try:
            uuid.UUID(uuid_list[idx]);
        except:
            uuid_list[idx] = str(uuid.uuid4())  #invalid UUID format, produce one
        print('%s,%s,%s'%(file, cluster_list[idx], uuid_list[idx]));
        idx += 1;
    
    line_count_list = ['1']*len(file_paths_list);

    for library in galaxy_instance.libraries.get_libraries(name=library_name):     #iterate over list of dict
      library_id = library['id'];
      library_client.upload_from_galaxy_filesystem(library_id, '\n'.join(file_paths_list), link_data_only='link_to_files',
          remote_dataset = True, uuid_list = '\n'.join(uuid_list), remote_dataset_type_list = '\n'.join(file_types_list),
          file_size_list = '\n'.join(file_sizes_list), line_count_list = '\n'.join(line_count_list) );


if __name__ == "__main__":
    if(len(sys.argv) < 3):
        sys.stderr.write('Needs 2 arguments : <Galaxy_library_name> <csv_file>\n');
        sys.exit(-1);
    #Connect to Galaxy
    gi = GalaxyInstance(url=galaxy_key.galaxy_host, key=galaxy_key.galaxy_key);
    li = LibraryClient(gi);

    library_name = sys.argv[1];
    csv_file = sys.argv[2];
    file_paths_list = [];
    file_sizes_list = [];
    file_types_list = [];
    cluster_list = [];      #g1.spark0.intel.com or g2.spark0.intel.com
    uuid_list = [];
    try:
        fptr = open(csv_file, 'rb');
    except:
        sys.stderr.write('Could not open CSV file\n');
        sys.exit(-1);
    csv_reader = csv.reader(fptr, delimiter=',');
    for row_tokens in csv_reader:
        if(len(row_tokens) > 0):
            if(row_tokens[0][0] == '#'):
                continue;
            assert len(row_tokens) >= 4;
            file_paths_list.append(row_tokens[0]);      #path
            file_sizes_list.append(row_tokens[1]);      #size in bytes
            file_types_list.append(row_tokens[2]);      #types - bam, sam, png etc
            cluster_list.append(row_tokens[3]);         #belongs to g1.spark0.intel.com or g2.spark0.intel.com
            if(len(row_tokens) >= 5):
                uuid_list.append(row_tokens[4]);        #CCC_DID - optional
            else:
                uuid_list.append('');                   #blank string - generate CCC_DID
    upload_to_galaxy_library(gi, li, library_name, file_paths_list, file_sizes_list, file_types_list, cluster_list, uuid_list);

