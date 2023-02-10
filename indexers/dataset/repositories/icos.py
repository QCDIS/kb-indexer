import json
import os
import shlex
import subprocess

import requests.exceptions

from ... import utils
from .common import Repository
from ..download import TwoStepDownloader
from ..map import Mapper, DeepSearch, flatten_list, get_html_tags
from ..index import Indexer


class ICOSDownloader(TwoStepDownloader):

    def record_urls(self):
        cURL = r"""curl https://meta.icos-cp.eu/sparql -H 'Cache-Control: no-cache' -X POST --data 'query=prefix%20cpmeta%3A%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fontologies%2Fcpmeta%2F%3E%0Aprefix%20prov%3A%20%3Chttp%3A%2F%2Fwww.w3.org%2Fns%2Fprov%23%3E%0Aselect%20%3Fdobj%20%3Fspec%20%3FfileName%20%3Fsize%20%3FsubmTime%20%3FtimeStart%20%3FtimeEnd%0Awhere%20%7B%0A%09VALUES%20%3Fspec%20%7B%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FradonFluxSpatialL3%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fco2EmissionInventory%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FsunInducedFluorescence%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FoceanPco2CarbonFluxMaps%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FinversionModelingSpatial%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FbiosphereModelingSpatial%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoFluxesDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoEcoDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoMeteoDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoAirTempMultiLevelsDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FecoProfileMultiLevelsDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcMeteoL0DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcLosGatosL0DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcPicarroL0DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FingosInversionResult%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fsocat_DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcBioMeteoRawSeriesBin%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcStorageFluxRawSeriesBin%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcBioMeteoRawSeriesCsv%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcStorageFluxRawSeriesCsv%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcSaheatFlagFile%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FceptometerMeasurements%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FglobalCarbonBudget%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FnationalCarbonEmissions%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FglobalMethaneBudget%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FdigHemispherPics%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcEddyFluxRawSeriesCsv%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcEddyFluxRawSeriesBin%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCh4L2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCoL2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCo2L2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcMtoL2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcC14L2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcMeteoGrowingNrtDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCo2NrtGrowingDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCh4NrtGrowingDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcN2oL2DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcCoNrtGrowingDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcN2oNrtGrowingDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FingosCh4Release%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FingosN2oRelease%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatcRnNrtDataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fdrought2018AtmoProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FmodelDataArchive%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcArchiveProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fdought2018ArchiveProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FatmoMeasResultsArchive%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcNrtAuxData%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcFluxnetProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2Fdrought2018FluxnetProduct%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcNrtFluxes%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcNrtMeteosens%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FetcNrtMeteo%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FicosOtcL1Product%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FicosOtcL1Product_v2%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FicosOtcL2Product%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FicosOtcFosL2Product%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FotcL0DataObject%3E%20%3Chttp%3A%2F%2Fmeta.icos-cp.eu%2Fresources%2Fcpmeta%2FinversionModelingTimeseries%3E%7D%0A%09%3Fdobj%20cpmeta%3AhasObjectSpec%20%3Fspec%20.%0A%09%3Fdobj%20cpmeta%3AhasSizeInBytes%20%3Fsize%20.%0A%3Fdobj%20cpmeta%3AhasName%20%3FfileName%20.%0A%3Fdobj%20cpmeta%3AwasSubmittedBy%2Fprov%3AendedAtTime%20%3FsubmTime%20.%0A%3Fdobj%20cpmeta%3AhasStartTime%20%7C%20%28cpmeta%3AwasAcquiredBy%20%2F%20prov%3AstartedAtTime%29%20%3FtimeStart%20.%0A%3Fdobj%20cpmeta%3AhasEndTime%20%7C%20%28cpmeta%3AwasAcquiredBy%20%2F%20prov%3AendedAtTime%29%20%3FtimeEnd%20.%0A%09FILTER%20NOT%20EXISTS%20%7B%5B%5D%20cpmeta%3AisNextVersionOf%20%3Fdobj%7D%0A%7D%0Aorder%20by%20desc%28%3FsubmTime%29'"""
        lCmd = shlex.split(cURL)  # Splits cURL into an array
        p = subprocess.Popen(
            lCmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()  # Get the output and the err message
        json_data = json.loads(r'' + out.decode("utf-8"))
        indexFile = open(self.paths.dataset_list_filename, "w+")
        indexFile.write(json.dumps(json_data))
        indexFile.close()
        print("ICOS data collection is done!")

        # ------------
        with open(self.paths.dataset_list_filename, "r") as f:
            data = json.load(f)

        urls = [record["dobj"]["value"]
                for record in data["results"]["bindings"]]

        with open(self.paths.dataset_urls_filename, 'w') as f:
            f.write('\n'.join(urls))

        # ------------
        with open(self.paths.dataset_urls_filename, 'r') as f:
            urls = [line.strip() for line in f.readlines()]
        return urls


class ICOSMapper(Mapper):
    contextual_text_fields = ["keywords", "genre", "theme", "name"]
    contextual_text_fallback_field = "Abstract"

    def gen_record_from_url(self, datasetURL):
        try:
            scripts = get_html_tags(datasetURL, "script")
        except requests.exceptions.ConnectionError:
            print(f'Could not open {datasetURL}, skipping')
            return

        with open(self.paths.metadataStar_filename, "r") as f:
            metadataStar_object = json.loads(f.read())

        indexfname = os.path.join(
            self.paths.index_records_dir,
            utils.gen_id_from_url(datasetURL) + '.json',
            )
        indexFile = open(indexfname, "w")
        indexFile.write("{\n")

        for script in scripts:
            if '<script type="application/ld+json">' in str(script):
                script = str(script)
                start = (script.find('<script type="application/ld+json">')
                         + len('<script type="application/ld+json">'))
                end = script.find("</script>")
                script = script[start:end]
                JSON = json.loads(r'' + script)
                RI = ""
                domains = ""
                topics = []
                originalValues = []
                cnt = 0
                for metadata_property in metadataStar_object:
                    cnt = cnt + 1

                    if metadata_property == "ResearchInfrastructure":
                        result = self.getRI(JSON)
                    elif metadata_property == "theme":
                        if not len(RI):
                            # RI= self.getRI(JSON)
                            RI = "ICOS"
                        if not len(domains):
                            domains = self.getDomain(RI)
                        if not len(topics):
                            topics = self.topicMining(JSON)
                        result = self.getTopicsByDomainVocabulareis(
                            topics, domains[0])
                    elif metadata_property == "potentialTopics":
                        if not len(topics):
                            topics = self.topicMining(JSON)
                        result = topics
                        result = self.pruneExtractedContextualInformation(
                            result, originalValues)
                    elif metadata_property == "EssentialVariables":
                        if not len(RI):
                            RI = self.getRI(JSON)
                        if not len(domains):
                            domains = self.getDomain(RI)
                        if not len(topics):
                            topics = self.topicMining(JSON)
                        essentialVariables = self.getDomainEssentialVariables(
                            domains[0])
                        result = self.getSimilarEssentialVariables(
                            essentialVariables, topics)
                        result = self.pruneExtractedContextualInformation(
                            result, originalValues)
                    elif metadata_property == "url":
                        result = datasetURL  # [str(datasetURL)]
                    else:
                        result = DeepSearch().search([metadata_property], JSON)
                        if not len(result):
                            searchFields = []
                            m = metadataStar_object[metadata_property]
                            for i in range(3, len(m)):
                                result = DeepSearch().search([m[i]], JSON)
                                if len(result):
                                    searchFields.append(result)
                            result = searchFields
                    propertyDatatype = metadataStar_object[metadata_property][
                        0]
                    # if metadata_property!="url":
                    result = self.refineResults(
                        result, propertyDatatype, metadata_property)

                    if cnt == len(metadataStar_object):
                        extrachar = "\n"
                    else:
                        extrachar = ",\n"

                    flattenValue = str(flatten_list(result))
                    if flattenValue == "[None]":
                        flattenValue = "[]"

                    if (
                            metadata_property == "description"
                            or metadata_property == "keywords"
                            or metadata_property == "abstract"):
                        txtVal = (flattenValue.replace("[", "")
                                  .replace("]", "")
                                  .replace("'", "")
                                  .replace("\"", "")
                                  .replace("\"\"", "")
                                  .replace("None", ""))
                        if txtVal != "":
                            originalValues.append(txtVal)

                    if metadata_property == "landingPage":
                        flattenValue = "[]"

                    indexFile.write(
                        "\""
                        + str(metadata_property)
                        + "\" :"
                        + flattenValue.replace("']", "\"]")
                        .replace("['", "[\"")
                        .replace("',", "\",")
                        .replace(", '", ", \"")
                        .replace("\"\"", "\"")
                        + extrachar
                        )

        indexFile.write("}")
        indexFile.close()


class ICOSRepository(Repository):
    name = 'ICOS'
    research_infrastructure = 'ICOS'

    downloader = ICOSDownloader
    mapper = ICOSMapper
    indexer = Indexer
