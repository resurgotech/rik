import dask as dk
import datashader as ds
import numba
import dask.array as da
from dask.distributed import Client
import h5py
import bioframe as bf
import pybedtools as pyb

client=Client()

data_dir = '/data1/'

dask_datafile = h5py.File('/data1/scATAC_blood.hdf','a') 
counts_dask =dask_datafile['/data/cd4']


all_atac_peaks = pyb.BedTool('GSE123578_isolates_peaks.bed')
bcbt_plusminus_20m = pyb.BedTool(data_dir+'bcl11b_40M.bed')

thymo_peaks = isolate_peaks.intersect(bcbt_plusmin_20m)
thymo_peaks.saveas('scatac_overlapping_thymod_broad_region.bed')

cd4_counts=pd.read_table('GSE123578_CD4_countsData.csv', sep= '\s+')
cd8_counts=pd.read_table('GSE123578_CD8_countsData.csv', sep= '\s+')
nk_counts=pd.read_table('GSE123578_NK_countsData.csv', sep= '\s+', index_col='peak_idx')

#peaks within broader ThymoD region
thymod_cd4s = cd4_counts.loc[(cd4_counts['peak_idx'] >= 105649) & (cd4_counts['peak_idx'] <= 107913)]

#separated into their separate cells - each as data table
thymo_c = pd.DataFrame([v for k, v in thymod_cd4s.groupby(['cell_idx'])])

