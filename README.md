# Project 3 – Spark Deliverables 3
Analysis of Novel Coronavirus strains |
|

| Krishna Shravya Gade | | CSci 5751 |
| --- | --- | --- |

 |

# Contents

[I. Introduction ](#_Toc39690119)

[II. Data ](#_Toc39690120)

[1. Datasets description ](#_Toc39690121)

[i) nCov strains dataset](#_Toc39690122)

[ii) COVID-19 case counts dataset](#_Toc39690123)

[2) Validation and Cleaning ](#_Toc39690124)

[3) Processing ](#_Toc39690125)

[i) nCov strains dataset](#_Toc39690126)

[iii) COVID-19 case counts dataset](#_Toc39690127)

[III. Analysis ](#_Toc39690128)

[1. Countries contributing strains for the study ](#_Toc39690129)

[a. Methods ](#_Toc39690130)

[b. Analysis ](#_Toc39690131)

[c. Result ](#_Toc39690132)

[2. Mutations vs case counts ](#_Toc39690133)

[a. Methods ](#_Toc39690134)

[b. Analysis ](#_Toc39690135)

[c. Result ](#_Toc39690136)

[3. Clustering strains ](#_Toc39690137)

[a. Methods ](#_Toc39690138)

[b. Analysis](#_Toc39690139)

[c. Result ](#_Toc39690140)

[IV. Challenges and lessons learnt ](#_Toc39690141)

[1. Challenges](#_Toc39690142)

[2. Lessons learnt](#_Toc39690143)

[V. Conclusion ](#_Toc39690144)

[VI. References](#_Toc39690145)

[VII. Appendix ](#_Toc39690146)


# I Introduction

In a battle with the COVID-19 disease, governments all over the world are trying to best understand the nature of the virus to mitigate consequences. Like the Zika virus which has been classified into three lineages and each one shows different symptoms, the Novel Coronavirus strains are being analysed by various biomedical labs all over the world to find a lineage tree or **phylogenetic tree**. This analysis might help governments deal with the virus differently from understanding the specific features of the strains.

This being the basis of this study, a dataset of most dominant strains all over the world has been combined with the COVID-19 cases and mortality dataset in those locations to find if a potential strain is more deadly or has less impact on the patient after infection.

The paper majorly covers the data manipulation and steps involved to validate the data available for each of the analysis. The methods used for analysis and conclusion are explained in detail for every analysis.

Below is a phylogenetic tree of the corona virus strains taken from GISAID website [4]:

![](RackMultipart20200507-4-1vdv1lb_html_d015da315c285934.png)

_Figure 1: Phylogenetic tree for corona virus strains [4]_


# II Data


## 1) Datasets description

Two datasets were used in the analysis – nCov strains dataset and COVID-19 case counts dataset. The detailed description of the attributes can be found in Appendix (VII 2). The details of the dataset are explained below:


### a. nCov strains dataset:

- Source: h-CoV-19 GISAID dataset [5]
- Size: 15800 records
- Format: TSV
- Relevant attributes: strain(string), date(date), country(string), division(string), host(string), originating lab(string)


### b. COVID-19 case counts dataset:

Initially, the time series version of this dataset was considered instead of the cumulative dataset. But given the dependencies between strains dataset and case counts dataset, it was observed that strains dataset lacked the richness required for a time series analysis. This will be covered in detail under Analysis section (III.3).

- Source: COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University [6]
- Size: 3192 rows
- Format: CSV
- Relevant attributes: province(string), country(string), confirmed(longint), deaths(longint), recovered(longint), active(longint)

## 2) Validation and Cleaning

Some common validations were done on both the datasets:

1. Number of null / na / missing values in every column of the data
2. Uniqueness of the &quot;key&quot; value
3. Total unique countries and divisions

## 3) Processing


### a. nCov strains dataset:

1. nCov strains data had _division\_exposure_ attribute which specified the division(state) where the patient was likely exposed to the virus. Upon exploring, it was found that this attribute was either same as _division_ attribute or was null for **92%** of the data meaning it did not carry any extra information than the _division_ attribute. Hence, only _division_ attribute was considered for analysis.
2. Strains data had non-human host strains which were the samples collected from bats, canines etc. These rows were removed from analysis as the motive behind the study is to analyse the effect of nCov on humans.


### b. COVID-19 case counts dataset:

1. Many countries in this COVID-19 case counts dataset were misspelled. Cleaning process is described in Analysis section (III 2. a.).


# III Analysis

1.
## Countries contributing strains to the dataset

The initial idea was to identify the specific virology labs contributing the most but due to inconsistencies in the &quot;_originating lab_&quot; field in the strains data, the study was pivoted towards countries.

  a.
### Methods

The processed strains data was categorized into countries that contributed the strains to the data set. This was done by running a group by query.

  b.
### Analysis

Below is the bar graph plot for the top 20 countries that contributed the most strains. United Kingdom, United States of America and Australia were the top three contributing countries.

![](RackMultipart20200507-4-1vdv1lb_html_47fa50cc9cd86ab.png)

  c.
### Result

The potential conclusion that can be drawn from the above visualization is that UK, USA and Australia are the countries researching to find a vaccine most extensively compared to other countries. The dip in the bar chart shows that USA and UK are leading substantially in comparison to other countries in terms of this research.

2.
## Mutations vs case counts

The motivation behind this analysis was to test the hypothesis if the confirmed cases in the country were correlated with the strains present in the country.

  a.
### Methods

- To weed out misspelled entries in COVID-19 counts dataset, the &quot;_country_&quot; attribute was subtracted from &quot;_country_&quot; in strain dataset. After finding possibly missing / wrongly spelled countries, **regular expression queries** were used to find the mapping of countries&#39; names between both the datasets. This also took care of the check if a country was missing in the COVID-19 counts dataset.
- To parallelize the process of updating all the country names, a dictionary mapping incorrect name with the correct one was casted to a **broadcast**** variable **which can be shared by all the tasks. A** user defined function** to get the values from the broadcast variable was written and called to update the country attribute.
- The counts data was grouped by _countries_ and the counts were aggregated to create a new data frame with country wise sum of _confirmed_, _death_s, _recovered_ and _active_ cases. This dataframe was **inner-joined** with the strains data upon the country column.

  b.
### Analysis

- **Pearson correlation coefficient** was calculated to be 0.5857 for _strains contributed_ and _confirmed_ cases. This is not a strong correlation hence a relationship cannot be established between these attributes.
- Below is the visual of the **scatter plot** for strains contributed by countries and confirmed cases in the country.

![](RackMultipart20200507-4-1vdv1lb_html_76bbb31485585b3b.png)

- There is no significant relationship between strains in the country and the confirmed cases as per the scatter plot above

  c.
### Result

- The weak correlation signifies the lack of any relationship between strains contributed by the country and the number of infections
- This could be due to various other factors such as lack of research investment by the governments of the countries, private research not publishing the genomes found or not enough tests being done and published to show real counts
- Only the strains that have high coverage
# 1
 were considered in the dataset and this could have contributed to the lack of enough data to establish a relationship

3.
## Clustering strains

The motivation behind this analysis was a research paper [2] published on bioRxiv about a speculation that the virus strain spreading in the west coast of the USA is less lethal than the one in east coast causing more deaths and severe symptoms. The idea was to study the lethality of each of the strains to classify them. This analysis could bolster the vaccine research helping scientists since individuals who have recovered could be susceptible to a mutant of the virus making vaccination a vain effort.

A time series data analysis with the date when the sample was collected and the case counts after that could have served as a good study but the strains from USA were only available for the month of January when the case counts were in single digits in the country. Hence study was pivoted to cumulative analysis instead. This is still significant as these strains could be the parent strains of the ones currently active in the region.

  a.
### Methods

    1.
#### Validating

- While validating the strains data, it was found that the highest detail about location was at county level for many strains but sufficiently enough records did not have that level of information. Hence the study was shifted to state level analysis to maintain uniformity.
- Some strains collected by University of Washington Virology lab had state as &quot;USA&quot; and these records were disregarded from the study
- After correcting the date strings for some entries, the date collected column was converted to date type. It was found that all strains collected in USA were only in the month of January 2020

    2.
#### Processing

- A new column called _category_ was created to classify the strains as older, mid and newer based on date strain was collected in the strains data
- The _COVID-19 case counts_ and _strains_ data was inner-joined based on the state column in both
- Using the Vector assembler package in spark ml lib the columns- _confirmed, deaths, recovered, active and category_ – were vectorised and added to a new column called _features_

  b.
### Analysis

- Once the dataframe was cleaned, processed and features were vectorized, a decision had to be made of choosing a model for the data
- **Kmeans** : Initially, Kmeans clustering model, from spark ML lib, was fit on the strains data to find similar strains but the clustering failed and resulted in insignificant classification. This could be attributed to the limitations of Kmeans clustering in context of the distribution of data points and presence of outliers in the data.
- **Gaussian Mixture Model:** Then the data was fit to a Gaussian Mixture Model to see some pattern in the clustering where the lower end of the spectrum with less confirmed cases and less deaths were considered one cluster, mid level where most strains were classified were one and higher numbers gave another cluster

Below is the plot of Gaussian Mixture Model clusters:

![](RackMultipart20200507-4-1vdv1lb_html_582689774aeefa32.png)

- **DBSCAN** : Lastly, the data was fit in DBSCAN model which rendered significant information in the classification. After multiple trials, the parameters chosen for DBSCAN were **epsilon of 1 and minimum samples in a cluster to be 20 gave around 388 clusters**. An epsilon with higher value could render a better prediction in our case but DBSCAN is a memory intensive algorithm and due to **memory limitation** in Databricks the process was getting terminated.

Below is the plot for the DBSCAN results:

![](RackMultipart20200507-4-1vdv1lb_html_8c98b48ff359a6a5.png)

- It is clear that the distribution of the datapoints in DBSCAN is similar to GMM but the clustering is more diverse and informative. We can visually see the strains that are comparatively less lethal are in brown, mid-level is yellow and the blue seem to be relatively more lethal.

  c.
### Result

Combining the results from DBSCAN and GMM, we could potentially claim that some strains spreading in the USA right now display different properties compared to others. Even though the results of the study could be skewed due to many other factors, the overall distribution of the strains and classification could point us to a direction. This result can be used by scientists to further probe into these strains protein composition to make other observations to support or disprove the claim.

IV.
# Challenges and lessons learnt

1.
## Challenges:

- The first major challenge for me was that the previous knowledge I had about genetics was not sufficient to deeply understand the data and perform analysis. I referred to biomed scientific research papers and articles which was both interesting and challenging for me as a computer science student.
- There were many inconsistencies in both the datasets and at each point when these were discovered, I had to **pivot the study** according to the data available
- The idea behind joining the two datasets on country had led me to probe into the **spelling mistakes, missing country names and anomalies** like one of the datasets considering Hong Kong as a state in China. It took quite a bit of time and effort to find these and fix them. Even though the COVID-19 case counts dataset had a FIPS to ISO codes for countries, Strains data did not have it which complicated things.

2.
## Lessons learnt:

- Running on a **hardware limitation of 16 GB** memory due to the limit on Databricks community version I learnt to rely on modifying the same dataframe instead of creating a new one each time to save memory. I also encountered memory issues while running DBSCAN clustering and adjusted the parameters to be memory efficient as well while delivering optimal clustering.

- Knowing enough about the subject matter is very essential for data analysis

V.
# Conclusion

The Novel Coronavirus strains analysis is a crucial step towards developing a vaccine and aiding decision making for governments all over the world. This study could have been proven with even better results if the data was richer and more labs all over the world actively contributed to the dataset. The lack of definite results in correlation between number of strains and confirmed cases in countries proves that not all countries are reporting numbers accurately and contributing to the greater good. Even though this is a proof by contradiction, more research could lead to conclusive results.

The second half of the analysis of finding a deadlier strain could serve as an early warning against a stronger and deadlier mutant of Coronavirus. A protein level analysis of these strains could give more specific features of the virus that could cause it to be more dangerous.

VI.
# References

[1] Structure of a Data Analysis Report
 http://www.stat.cmu.edu/~brian/701/notes/paper-structure.pdf

[2] Spike mutation pipeline reveals the emergence of a more transmissible form of SARS-CoV-2 [https://www.biorxiv.org/content/10.1101/2020.04.29.069054v1](https://www.biorxiv.org/content/10.1101/2020.04.29.069054v1)

[3] Mutant coronavirus strain has emerged that&#39;s even more contagious than original, study says [https://www.post-gazette.com/news/science/2020/05/05/coronavirus-strain-study-scientists-new-mutant-more-contagious/stories/202005050156](https://www.post-gazette.com/news/science/2020/05/05/coronavirus-strain-study-scientists-new-mutant-more-contagious/stories/202005050156)

[4] [https://www.epicov.org/epi3/frontend#lightbox1597646798](https://www.epicov.org/epi3/frontend#lightbox1597646798)

[5] Genomic epidemiology [https://www.epicov.org/epi3/frontend#5e283e](https://www.epicov.org/epi3/frontend#5e283e)

[6] COVID-19 Data Repository by the Center for Systems Science and Engineering (CSSE) at Johns Hopkins University [https://github.com/CSSEGISandData/COVID-19](https://github.com/CSSEGISandData/COVID-19)

[7] How genomic epidemiology is tracking the spread of COVID-19 locally and globally [https://cen.acs.org/biological-chemistry/genomics/genomic-epidemiology-tracking-spread-COVID/98/i17](https://cen.acs.org/biological-chemistry/genomics/genomic-epidemiology-tracking-spread-COVID/98/i17)

[8] Reconstructing evolutionary trees in parallel for massive sequences [https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5751538/](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5751538/)

[9] Stackoverflow.com

[10] Spark documentation [https://spark.apache.org/docs/2.2.0/ml-clustering.html](https://spark.apache.org/docs/2.2.0/ml-clustering.html)

[11] [https://dwgeek.com/](https://dwgeek.com/)

[12] Next strain data description [https://github.com/nextstrain/ncov/blob/master/docs/metadata.md](https://github.com/nextstrain/ncov/blob/master/docs/metadata.md)

[13] CSSEGISandData data description [https://github.com/CSSEGISandData/COVID-19/tree/master/csse\_covid\_19\_data](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data)

VII.
# Appendix

Link to the databricks code: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3847040375540205/1221180919542404/3031952302722608/latest.html

1.
## Acknowledgement:

Strains dataset: I gratefully acknowledge the authors and researchers in originating and submitting labs of sequence data on which the analysis is based and GISAID for hosting and publishing the dataset.

COVID-19 counts dataset: I gratefully acknowledge Center for Systems Science and Engineering (CSSE) at Johns Hopkins University for providing the data to support this analysis.

2.
## Data description:

1. Strains data [12]

  1. **Column 1: **** strain**

  1. **Column 2: **** virus**

Type of virus – nCov in our case

  1. **Column 3: **** gisaid\_epi\_isl**

If this genome is shared via [GISAID](https://www.gisaid.org/) then please include the EPI ISL here. In our example this is &quot;EPI\_ISL\_413490&quot;.

  1. **Column 4: **** genbank\_accession**

If this genome is shared via [GenBank](https://www.ncbi.nlm.nih.gov/genbank/) then please include the accession number here. In our example this is &quot;?&quot; indicating that it hasn&#39;t (yet) been deposited in GenBank. (See above for more information on how to encode missing data.)

  1. **Column 5: **** date** (really important!)

This describes the sample collection data (_not_ sequencing date!) and must be formated according as YYYY-MM-DD.

  1. **Column 6: **** region**

The region the sample was collected in -- for our example this is &quot;Oceania&quot;. Please use either &quot;Africa&quot;, &quot;Asia&quot;, &quot;Europe&quot;, &quot;North America&quot;, &quot;Oceania&quot; or &quot;South America&quot;.

  1. **Column 7: **** country**

The country the sample was collected in

  1. **Column 8: **** division**

Division currently doesn&#39;t have a precise definition and we use it differently for different regions. For instance for samples in the USA, division is the state in which the sample was collected here. For other countries, it might be a county, region, or other administrative sub-division. To see the divisions which are currently set for your country, you can run the following command (replace &quot;New Zealand&quot; with your country):

  1. **Column 9: **** location**

Similarly to division, but for a smaller geographic resolution. This data is often unavailable, and missing data here is typically represented by an empty field or the same value as division is used.

  1. **Column 10: **** region\_exposure**

If the sample has a known travel history and infection is thought to have occurred in this location, then represent this here.

If there is no travel history then set this to be the same value as region.

  1. **Column 11: **** country\_exposure**

Analogous to region\_exposure but for country. In our example, given the patient&#39;s travel history, this is set to &quot;Iran&quot;.

  1. **Column 12: **** division\_exposure**

Analogous to region\_exposure but for division. If we don&#39;t know the exposure division, we may specify the value for country\_exposure here as well.

  1. **Column 13: **** segment**

Unused. Typically the value &quot;genome&quot; is set here.

  1. **Column 14: **** length**

Unused : Genome length (numeric value).

  1. **Column 15: **** host**

Host from which the sample was collected. Currently we have multiple values in the dataset, including &quot;Human&quot;, &quot;Canine&quot;, &quot;Manis javanica&quot; and &quot;Rhinolophus affinis&quot;.

  1. **Column 16: **** age**

Numeric age of the patient from whom the sample was collected. We round to an integer value.

  1. **Column 17: **** sex**

Sex of the patient from whom the sample was collected.

  1. **Column 18: **** originating\_lab**
  2. Please see [GISAID](https://www.gisaid.org/help/publish-with-gisaid-references/) for more information.
  3. **Column 19: **** submitting\_lab**
  4. Please see [GISAID](https://www.gisaid.org/help/publish-with-gisaid-references/) for more information.
  5. **Column 20: **** authors**

Author of the genome sequence, or the paper which announced this genome. Typically written as &quot;LastName et al&quot;.

  1. **Column 21: **** url**

The URL, if available, pointing to the genome data. For most SARS-CoV-2 data this is [https://www.gisaid.org](https://www.gisaid.org/).

  1. **Column 22: **** title**

The URL, if available, of the publication announcing these genomes.

  1. **Column 23: **** date\_submitted**

Date the genome was submitted to a public database (most often GISAID). In YYYY-MM-DD format (see date for more information on this formatting).

1. COVID-19 case counts [13]:

1. **FIPS** : US only. Federal Information Processing Standards code that uniquely identifies counties within the USA.
2. **Admin2** : County name. US only.
3. **Province\_State** : Province, state or dependency name.
4. **Country\_Region** : Country, region or sovereignty name. The names of locations included on the Website correspond with the official designations used by the U.S. Department of State.
5. **Last Update** : MM/DD/YYYY HH:mm:ss (24 hour format, in UTC).
6. **Lat**  and  **Long\_** : Dot locations on the dashboard. All points (except for Australia) shown on the map are based on geographic centroids, and are not representative of a specific address, building or any location at a spatial scale finer than a province/state. Australian dots are located at the centroid of the largest city in each state.
7. **Confirmed** : Confirmed cases include presumptive positive cases and probable cases, in accordance with CDC guidelines as of April 14.
8. **Deaths** : Death totals in the US include confirmed and probable, in accordance with [CDC](https://www.cdc.gov/coronavirus/2019-ncov/cases-updates/cases-in-us.html) guidelines as of April 14.
9. **Recovered** : Recovered cases outside China are estimates based on local media reports, and state and local reporting when available, and therefore may be substantially lower than the true number. US state-level recovered cases are from [COVID Tracking Project](https://covidtracking.com/).
10. **Active:**  Active cases = total confirmed - total recovered - total deaths.
11. **Incidence\_Rate** : Admin2 + Province\_State + Country\_Region.
12. **Case-Fatality Ratio (%)**: = confirmed cases per 100,000 persons.
13. **US Testing Rate** : = total test results per 100,000 persons. The &quot;total test results&quot; is equal to &quot;Total test results (Positive + Negative)&quot; from [COVID Tracking Project](https://covidtracking.com/).
14. **US Hospitalization Rate (%)**: = Total number hospitalized / Number confirmed cases. The &quot;Total number hospitalized&quot; is the &quot;Hospitalized – Cumulative&quot; count from [COVID Tracking Project](https://covidtracking.com/). The &quot;hospitalization rate&quot; and &quot;hospitalized - Cumulative&quot; data is only presented for those states which provide cumulative hospital data.

[1](#sdfootnote1anc) Coverage of a sequence is the average number of reads to encode the genome sequence. High coverage in case of this dataset implies there are less than 0.1% Ns which indicate unknown nucleotide base. N/A in computer science terminology.

[2](#sdfootnote2anc) FIPS and ISO codes are internationally recognized codes for states in the USA and countries respectively
