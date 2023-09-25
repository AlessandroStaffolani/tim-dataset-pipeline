import argparse

from dataset.preprocessing.bs_pipeline import process_base_stations
from dataset.preprocessing.chunks_pipeline import process_chunks


def chunks_pipeline_args(module_parser, module_name):
    parser = module_parser.add_parser(module_name, help=f'{module_name} dataset chunks')
    parser.add_argument('input', help='Input path for the metadata file')
    parser.add_argument('output', help='Output path for saving the chunks')
    parser.add_argument('-n', '--n-chunks', default=-1, type=int,
                        help='Number of chunks to load. -1 for all of them.')
    parser.add_argument('--skip', default=0, type=int, help='Set how many chunks you want to skip.')
    parser.add_argument('--server-url', default='dataverse.harvard.edu',
                        help='Data server url (without protocol). Default: "dataverse.harvard.edu"')
    parser.add_argument('--protocol', default='https',
                        help='Protocol used for the http requests. Default: "https"')
    parser.add_argument('--bs-aggregation-step', default=False, action='store_true',
                        help='Complete the pipeline by aggregating the traces with the correspondent aggregated BS')
    parser.add_argument('--aggregated-bs-file',
                        help='Path to the csv with the aggregated BS. (result of the bs script)')
    parser.add_argument('--full-aggregation', action='store_true', default=False,
                        help='If present, the aggregation will be performed using all the columns')
    parser.add_argument('--skip-download', action='store_true', default=False,
                        help='If present it will assume the chunks as already downloaded')


def cell_bs_pipeline_args(module_parser):
    parser = module_parser.add_parser('bs', help='Base Stations pipeline')
    parser.add_argument('input', help='Input path for the grid geojson file')
    parser.add_argument('output_folder', help='Output folder path for saving the chunks')
    parser.add_argument('--api-path', default='https://opencellid.org/ajax/getCells.php',
                        help='Url of the providers for downloading the base stations')
    parser.add_argument('--api-token', default='',
                        help='Base station provider token')
    parser.add_argument('--box-side', default=10, type=int,
                        help='Dimension of one box side used for grouping and downloading the base stations')
    parser.add_argument('--sleep-interval', default=0, type=float,
                        help='Interval between subsequent call to the base station provider')
    parser.add_argument('--collection', required=True, help='Name of the collection used for saving the data')
    parser.add_argument('--skip-bs-download', action='store_true', default=False,
                        help='If present it will skip the download of the base stations from the provider')
    parser.add_argument('--bs-types', default='LTE', help='Comma separated list of types of base station to process')
    parser.add_argument('--skip-db-upload', default=False, action='store_true',
                        help='If present it will skip to upload on the MongoDB')


def parse_arguments():
    main_parser = argparse.ArgumentParser(description='Dataset utility')
    module_parser = main_parser.add_subparsers(dest='module', title='Module', required=True)
    cell_bs_pipeline_args(module_parser)
    chunks_pipeline_args(module_parser, 'chunks-pipeline')
    return main_parser.parse_args()


if __name__ == '__main__':
    args = parse_arguments()
    if args.module == 'chunks-pipeline':
        process_chunks(args)
    if args.module == 'bs':
        process_base_stations(args)
