import json
import urllib.parse
import requests
import textwrap

from ... import utils
from .common import Repository
from ..download import TwoStepDownloader
from ..convert import Converter, DeepSearch, flatten_list, get_html_tags
from ..index import Indexer


class ICOSDownloader(TwoStepDownloader):
    documents_list_url = 'https://meta.icos-cp.eu/sparql'
    document_extension = '.html'

    def get_documents_urls(self):
        sparql_query = r"""
            prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
            prefix prov: <http://www.w3.org/ns/prov#>
            select ?dobj ?spec ?fileName ?size ?submTime ?timeStart ?timeEnd
            where {
                VALUES ?spec {<http://meta.icos-cp.eu/resources/cpmeta/radonFluxSpatialL3> <http://meta.icos-cp.eu/resources/cpmeta/co2EmissionInventory> <http://meta.icos-cp.eu/resources/cpmeta/sunInducedFluorescence> <http://meta.icos-cp.eu/resources/cpmeta/oceanPco2CarbonFluxMaps> <http://meta.icos-cp.eu/resources/cpmeta/inversionModelingSpatial> <http://meta.icos-cp.eu/resources/cpmeta/biosphereModelingSpatial> <http://meta.icos-cp.eu/resources/cpmeta/ecoFluxesDataObject> <http://meta.icos-cp.eu/resources/cpmeta/ecoEcoDataObject> <http://meta.icos-cp.eu/resources/cpmeta/ecoMeteoDataObject> <http://meta.icos-cp.eu/resources/cpmeta/ecoAirTempMultiLevelsDataObject> <http://meta.icos-cp.eu/resources/cpmeta/ecoProfileMultiLevelsDataObject> <http://meta.icos-cp.eu/resources/cpmeta/atcMeteoL0DataObject> <http://meta.icos-cp.eu/resources/cpmeta/atcLosGatosL0DataObject> <http://meta.icos-cp.eu/resources/cpmeta/atcPicarroL0DataObject> <http://meta.icos-cp.eu/resources/cpmeta/ingosInversionResult> <http://meta.icos-cp.eu/resources/cpmeta/socat_DataObject> <http://meta.icos-cp.eu/resources/cpmeta/etcBioMeteoRawSeriesBin> <http://meta.icos-cp.eu/resources/cpmeta/etcStorageFluxRawSeriesBin> <http://meta.icos-cp.eu/resources/cpmeta/etcBioMeteoRawSeriesCsv> <http://meta.icos-cp.eu/resources/cpmeta/etcStorageFluxRawSeriesCsv> <http://meta.icos-cp.eu/resources/cpmeta/etcSaheatFlagFile> <http://meta.icos-cp.eu/resources/cpmeta/ceptometerMeasurements> <http://meta.icos-cp.eu/resources/cpmeta/globalCarbonBudget> <http://meta.icos-cp.eu/resources/cpmeta/nationalCarbonEmissions> <http://meta.icos-cp.eu/resources/cpmeta/globalMethaneBudget> <http://meta.icos-cp.eu/resources/cpmeta/digHemispherPics> <http://meta.icos-cp.eu/resources/cpmeta/etcEddyFluxRawSeriesCsv> <http://meta.icos-cp.eu/resources/cpmeta/etcEddyFluxRawSeriesBin> <http://meta.icos-cp.eu/resources/cpmeta/atcCh4L2DataObject> <http://meta.icos-cp.eu/resources/cpmeta/atcCoL2DataObject> <http://meta.icos-cp.eu/resources/cpmeta/atcCo2L2DataObject> <http://meta.icos-cp.eu/resources/cpmeta/atcMtoL2DataObject> <http://meta.icos-cp.eu/resources/cpmeta/atcC14L2DataObject> <http://meta.icos-cp.eu/resources/cpmeta/atcMeteoGrowingNrtDataObject> <http://meta.icos-cp.eu/resources/cpmeta/atcCo2NrtGrowingDataObject> <http://meta.icos-cp.eu/resources/cpmeta/atcCh4NrtGrowingDataObject> <http://meta.icos-cp.eu/resources/cpmeta/atcN2oL2DataObject> <http://meta.icos-cp.eu/resources/cpmeta/atcCoNrtGrowingDataObject> <http://meta.icos-cp.eu/resources/cpmeta/atcN2oNrtGrowingDataObject> <http://meta.icos-cp.eu/resources/cpmeta/ingosCh4Release> <http://meta.icos-cp.eu/resources/cpmeta/ingosN2oRelease> <http://meta.icos-cp.eu/resources/cpmeta/atcRnNrtDataObject> <http://meta.icos-cp.eu/resources/cpmeta/drought2018AtmoProduct> <http://meta.icos-cp.eu/resources/cpmeta/modelDataArchive> <http://meta.icos-cp.eu/resources/cpmeta/etcArchiveProduct> <http://meta.icos-cp.eu/resources/cpmeta/dought2018ArchiveProduct> <http://meta.icos-cp.eu/resources/cpmeta/atmoMeasResultsArchive> <http://meta.icos-cp.eu/resources/cpmeta/etcNrtAuxData> <http://meta.icos-cp.eu/resources/cpmeta/etcFluxnetProduct> <http://meta.icos-cp.eu/resources/cpmeta/drought2018FluxnetProduct> <http://meta.icos-cp.eu/resources/cpmeta/etcNrtFluxes> <http://meta.icos-cp.eu/resources/cpmeta/etcNrtMeteosens> <http://meta.icos-cp.eu/resources/cpmeta/etcNrtMeteo> <http://meta.icos-cp.eu/resources/cpmeta/icosOtcL1Product> <http://meta.icos-cp.eu/resources/cpmeta/icosOtcL1Product_v2> <http://meta.icos-cp.eu/resources/cpmeta/icosOtcL2Product> <http://meta.icos-cp.eu/resources/cpmeta/icosOtcFosL2Product> <http://meta.icos-cp.eu/resources/cpmeta/otcL0DataObject> <http://meta.icos-cp.eu/resources/cpmeta/inversionModelingTimeseries>}
                ?dobj cpmeta:hasObjectSpec ?spec .
                ?dobj cpmeta:hasSizeInBytes ?size .
                ?dobj cpmeta:hasName ?fileName .
                ?dobj cpmeta:wasSubmittedBy/prov:endedAtTime ?submTime .
                ?dobj cpmeta:hasStartTime | (cpmeta:wasAcquiredBy / prov:startedAtTime) ?timeStart .
                ?dobj cpmeta:hasEndTime | (cpmeta:wasAcquiredBy / prov:endedAtTime) ?timeEnd .
                FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
            }
            order by desc(?submTime)
            """
        sparql_query = textwrap.dedent(sparql_query).strip()

        r = requests.post(
            self.documents_list_url,
            headers={'Cache-Control': 'no-cache'},
            data={'query': sparql_query},
            )
        data = r.json()

        urls = [record["dobj"]["value"]
                for record in data["results"]["bindings"]]
        return urls


class ICOSConverter(Converter):
    contextual_text_fields = ["keywords", "genre", "theme", "name"]
    contextual_text_fallback_field = "Abstract"

    def convert_record(self, raw_filename, converted_filename, metadata):
        with open(self.paths.metadataStar_filename, "r") as f:
            metadataStar_object = json.loads(f.read())

        with open(raw_filename, 'rb') as f:
            scripts = get_html_tags(f, "script")

        indexFile = open(converted_filename, "w")
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
                        result = metadata['url']
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
    converter = ICOSConverter
    indexer = Indexer
