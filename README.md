# SSH Open Marketplace Data Library

This repository contains a Python library to download and process the [SSH Open Marketplace](https://marketplace.sshopencloud.eu/) dataset, and a set of notebooks providing examples and use cases to use this library. 

The SSH Open Marketplace Data Library has been designed to be used by the SSH Open Marketplace Editorial Team and provides a set of ad hoc functions that can be used in Python Notebooks or programs. The various notebooks included in this repository allow any user to gain an overview of the SSH Open Marketplace (notebook 2) and authenticated users to write back to the SSH Open Marketplace specific curation information. See the [SSH Open Marketplace user documentation](https://marketplace.sshopencloud.eu/contribute/moderator-guidelines) for more details.
 
## Usage

To use the library functionalities: 

A - Create an instance of mplib.MPData, and load locally the MP data. The function:

##### getMPItems (category: str, local: boolean) -> DataFrame

downloads MP dataset and store it locally. The data is provided as a Data Frame i.e. data is organized in a tabular fashion and columns are labeled with the names of the attribites in MP datamodel.


 Example:
```Python 
 from sshmarketplacelib import MPData as mpd

 mpdata = mpd()
 ts_df=mpdata.getMPItems ("pubblications", True)
```

the data is returned as a Data Frame:

<table>
<tr>
	<th>id</th><th>	category</th><th>	label</th><th>	persistentId</th><th>	lastInfoUpdate</th><th>	status</th><th>	description</th><th>	contributors</th><th>	properties</th><th>	externalIds</th>
    </tr><tr>
<td>10414</td><td>	publication</td><td>	3D-ICONS -- 3D Digitisation of Icons of Europe...</td><td>	jOum8c</td><td>	2021-06-23T17:03:55+0000</td><td>	approved</td><td>	3D-ICONS was a pilot project funded under the ...	</td><td>[]</td><td>	[{'id': 41261, 'type': {'code': 'language', 'l...	</td><td>[]</td>
 </tr><tr>
<td>7454</td><td>	publication</td><td>	4 Default Text Structure - The TEI Guidelines</td><td>	Y3Vmhy</td><td>	2021-06-22T13:30:43+0000</td><td>	approved</td><td>	No description provided.</td><td>	[]</td><td>	[{'id': 41094, 'type': {'code': 'language', 'l...	</td><td>[]</td>
 </tr><tr>
<td>10738</td><td>	publication</td><td>	9 Dictionaries - The TEI Guidelines</td><td>	vQ7Bvs</td><td>	2021-06-23T17:04:34+0000</td><td>	approved</td><td>	No description provided.</td><td>	[]</td><td>	[{'id': 41163, 'type': {'code': 'language', 'l...	</td><td>[]</td><td>
 </tr>
</table>


B - Use the helper functions to analyse the Market Place data, for example the function below returns the number of null values for all propertes in each item category:

##### getNullValues()-> DataFrame


Example:

```Python 
 from sshmarketplacelib import helper as hlpr

 utils = hlpr.Util()
 nv_df=utils.getNullValues()
```


Returns:

<TABLE>
    <tr>
    <th>category <br>property: missed values</th><th>dataset</th><th>publication</th><th>tool-or-service</th><th>	training-material</th><th>	workflow</th>
        </tr><tr>
<td>accessibleAt</td><td>	1</td><td>	7</td><td>	475</td><td>	14</td><td>	1</td>
    </tr><tr>
<td>composedOf</td><td>	305</td><td>	137</td><td>	1671</td><td>	321</td><td>	0</td>
    </tr><tr>
<td>concept.candidate</td><td>	46</td><td>	5</td><td>	157</td><td>	0</td><td>	0</td>
    </tr>
    <tr>
<td>...</td><td>...</td><td>...</td><td>...</td><td>...</td><td>...</td>
    </tr>

</TABLE> 
  

## Installation
It is recommended to install library in a virtual environment to avoid dependency clash. 
To install the library enter cloned directory and install it via pip with explicit *requirements.txt* from the project:

- Clone the repository, enter the directory and install requirements:
```bash
git clone https://github.com/SSHOC/marketplace-curation.git
cd marketplace-curation
pip install ./ -r ./requirements.txt
```

- Edit the config.yaml.template file and set the values, then rename the file as *config.yaml*

- Create a folder called 'data' in the same folder of your notebooks/programs


