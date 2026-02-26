package it.eng.idra.connectors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import it.eng.idra.beans.dcat.*;
import it.eng.idra.beans.odms.*;
import it.eng.idra.utils.NbsRegistryConnectorUtils;
import it.eng.idra.utils.NbsRegistryConnectorUtils.NbsConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Connector for Urbreath NBS Registry.
 *
 * - Authenticate: POST https://services-dev.urbreath.tech/api/users/authenticate
 * - Brief: POST {baseUrl}/api/nbs/brief?page..&size..&sort..&direction.. with JSON body filters
 * - Detail: GET  {baseUrl}/api/nbs/{id}
 *
 * Images are ignored for now.
 */
public class NbsRegistryConnector implements IodmsConnector {

    private static final Logger logger = LogManager.getLogger(NbsRegistryConnector.class);

    private final OdmsCatalogue node;
    private final String nodeId;

    private final HttpClient http;
    private final Gson gson;

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(60);

    public NbsRegistryConnector(OdmsCatalogue node) {
        this.node = node;
        this.nodeId = String.valueOf(node.getId());
        this.http = HttpClient.newBuilder()
                .connectTimeout(CONNECT_TIMEOUT)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        this.gson = new GsonBuilder().disableHtmlEscaping().create();
    }

    /* =====================
     * IodmsConnector methods
     * ===================== */

    @Override
    public int countDatasets() throws Exception {
        return getAllDatasets().size();
    }

    @Override
    public List<DcatDataset> getAllDatasets() throws Exception {
    	NbsRegistryConnectorUtils.NbsConfig cfg =
    		    NbsRegistryConnectorUtils.readNbsConfig(node.getConnectorParams());
    	
    	//logger test 800A
    	logger.info("[NBS] Harvest start: nodeId={}, host={}, homepage={}",
    	        nodeId, node.getHost(), node.getHomepage());

    	logger.info("[NBS] Config: filterMode={}, idsCount={}, onlyUrbreathNbs={}, language={}, pageSize={}, sort={}, direction={}",
    	        cfg.filter.mode,
    	        cfg.filter.ids != null ? cfg.filter.ids.size() : 0,
    	        cfg.filter.onlyUrbreathNbs,
    	        cfg.filter.languageCode,
    	        cfg.paging.pageSize,
    	        cfg.paging.sort,
    	        cfg.paging.direction);
    	//logger test 800A
    	
    	String token = authenticate(cfg);
        String proxyBaseUrl= cfg.proxyBaseUrl;

        List<NbsBriefItem> allBrief = new ArrayList<>();
        int page = 0;

        while (true) {
            BriefResponse brief = fetchBriefPage(token, cfg, page);
            if (brief == null || brief.data == null) break;

            List<NbsBriefItem> results = (brief.data.results != null) ? brief.data.results : Collections.emptyList();
            allBrief.addAll(results);

            boolean last = Boolean.TRUE.equals(brief.data.lastPage);
            if (last) break;
            page++;
        }

        // Dedup IDs (defensive)
        LinkedHashMap<String, NbsBriefItem> byId = new LinkedHashMap<>();
        for (NbsBriefItem it : allBrief) {
            if (it == null || it.id == null) continue;
            byId.putIfAbsent(String.valueOf(it.id), it);
        }

        List<DcatDataset> out = new ArrayList<>();
        for (String id : byId.keySet()) {
            NbsDetailResponse detail = fetchDetail(token, id, cfg);
            if (detail == null || detail.data == null) continue;
            out.add(mapNbsToDcat(detail.data, apiBaseUrl(cfg), proxyBaseUrl));
        }

        return dedupByIdentifier(out);
    }

    @Override
    public DcatDataset getDataset(String datasetId) throws Exception {
    	NbsRegistryConnectorUtils.NbsConfig cfg =
    		    NbsRegistryConnectorUtils.readNbsConfig(node.getConnectorParams());

        String token = authenticate(cfg);
        NbsDetailResponse detail = fetchDetail(token, datasetId, cfg);
        String proxyBaseUrl= cfg.proxyBaseUrl;
        if (detail == null || detail.data == null) return null;
        return mapNbsToDcat(detail.data, apiBaseUrl(cfg), proxyBaseUrl);
    }

    @Override
    public List<DcatDataset> findDatasets(HashMap<String, Object> searchParameters) throws Exception {
        // Minimal approach: harvest configured scope, then filter in-memory by "q" if present.
        List<DcatDataset> all = getAllDatasets();
        if (searchParameters == null || searchParameters.isEmpty()) return all;

        Object qObj = searchParameters.get("q");
        if (qObj == null) return all;

        String q = String.valueOf(qObj).trim().toLowerCase();
        if (q.isEmpty()) return all;

        return all.stream()
                .filter(d -> {
                    String title = d.getTitle() != null ? String.valueOf(d.getTitle().getValue()) : "";
                    String desc  = d.getDescription() != null ? String.valueOf(d.getDescription().getValue()) : "";
                    return title.toLowerCase().contains(q) || desc.toLowerCase().contains(q);
                })
                .collect(Collectors.toList());
    }

    @Override
    public int countSearchDatasets(HashMap<String, Object> searchParameters) throws Exception {
        return findDatasets(searchParameters).size();
    }

    @Override
    public OdmsSynchronizationResult getChangedDatasets(List<DcatDataset> oldDatasets, String startingDate) throws Exception {
        // As agreed: ignore "changed", compute only added/deleted.
        List<DcatDataset> newDatasets = getAllDatasets();

        OdmsSynchronizationResult r = new OdmsSynchronizationResult();

        Set<DcatDataset> newSet = new HashSet<>(newDatasets);
        Set<DcatDataset> oldSet = new HashSet<>(oldDatasets);

        for (DcatDataset d : newSet) if (!oldSet.contains(d)) r.addToAddedList(d);
        for (DcatDataset d : oldSet) if (!newSet.contains(d)) r.addToDeletedList(d);

        return r;
    }

    @Override
    public DcatDataset datasetToDcat(Object dataset, OdmsCatalogue node) throws Exception {
        if (!(dataset instanceof NbsData)) return null;
        NbsRegistryConnectorUtils.NbsConfig cfg =
                NbsRegistryConnectorUtils.readNbsConfig(node.getConnectorParams());
        String proxyBaseUrl= cfg.proxyBaseUrl;

        return mapNbsToDcat((NbsData) dataset, apiBaseUrl(cfg), proxyBaseUrl);
    }

    /* =====================
     * Mapping
     * ===================== */

    private DcatDataset mapNbsToDcat(NbsData nbs, String apiBase, String proxyBaseUrl) {
        String identifier = nbs.id != null ? String.valueOf(nbs.id) : "";
        String title = safe(nbs.title, identifier);

        String releaseDate = NbsRegistryConnectorUtils.epochMillisToIsoZ(nbs.dateCreated);
        String updateDate = releaseDate; // no "modified" -> keep stable

        String description = NbsRegistryConnectorUtils.buildLongDescription(nbs);

        // Keywords include pilot/climateZone as agreed, provided they are available during harvesting
        List<String> keywords = new ArrayList<>();
        if (nbs.keywords != null) keywords.addAll(nbs.keywords);
        if (nbs.problems != null) keywords.addAll(nbs.problems);
        if (StringUtils.isNotBlank(nbs.pilot)) keywords.add("pilot:" + nbs.pilot);
        if (StringUtils.isNotBlank(nbs.climateZone)) keywords.add("climateZone:" + nbs.climateZone);
        keywords = NbsRegistryConnectorUtils.dedupNonEmpty(keywords);

        // Spatial point (lat,lon) + address as label (if present)
        DctLocation spatial = null;
        if (nbs.geoLocation != null && nbs.geoLocation.latitude != null && nbs.geoLocation.longitude != null) {
            String point = nbs.geoLocation.latitude + "," + nbs.geoLocation.longitude;
            spatial = new DctLocation(DCTerms.spatial.getURI(), "", safe(nbs.geoLocation.address, ""), point, nodeId, point, point);
        }

        FoafAgent publisher = new FoafAgent(
                DCTerms.publisher.getURI(),
                null,
                List.of(safe(node.getPublisherName(), "Urbreath")),
                null, null, null, null,
                nodeId
        );

        List<VcardOrganization> contactPointList = new ArrayList<>();
        if (StringUtils.isNotBlank(node.getPublisherEmail())) {
            VcardOrganization contact = new VcardOrganization(
                    DCAT.contactPoint.getURI(),
                    safe(node.getHomepage(), ""),
                    safe(node.getPublisherName(), "Urbreath"),
                    node.getPublisherEmail(),
                    "", "", "",
                    nodeId
            );
            contactPointList.add(contact);
        }

        List<SkosConceptTheme> themes = new ArrayList<>();
        List<SkosConceptSubject> subjects = new ArrayList<>();

        String landingPage = buildLandingPage(nbs);

        // Distributions (NO IMAGES)
        List<DcatDistribution> distributions = new ArrayList<>();
              
        // Always include a JSON distribution pointing to the API detail endpoint
        distributions.add(buildJsonDistribution(identifier, apiBase));

        // Video distributions (if any)
        if (nbs.videoUrls != null) {
            for (String url : nbs.videoUrls) {
                if (StringUtils.isBlank(url)) continue;
                distributions.add(buildLinkDistribution(url, "video", null));
            }
        }

        // Related materials
        if (nbs.relatedMaterial != null) {
            for (String url : nbs.relatedMaterial) {
                if (StringUtils.isBlank(url)) continue;
                distributions.add(buildLinkDistribution(url, "related material", null));
            }
        }

        String accessRightsUri = "http://publications.europa.eu/resource/authority/access-right/PUBLIC";

        return new DcatDataset(
                nodeId,
                identifier,
                title,
                description,
                distributions,
                themes,
                publisher,
                contactPointList,
                keywords,
                accessRightsUri,
                new ArrayList<DctStandard>(),  // conformsTo
                new ArrayList<String>(),       // documentation
                null,                          // frequency
                new ArrayList<String>(),       // hasVersion
                new ArrayList<String>(),       // isVersionOf
                landingPage,
                new ArrayList<String>(),       // languages
                new ArrayList<String>(),       // provenance
                releaseDate,
                updateDate,
                new ArrayList<String>(),       // otherIdentifier
                new ArrayList<String>(),       // sample
                new ArrayList<String>(),       // source
                (spatial == null ? new ArrayList<DctLocation>() : List.of(spatial)),
                null,                          // temporal
                "",                            // type
                "",                            // version
                new ArrayList<String>(),       // versionNotes
                null,                          // rightsHolder
                null,                          // creator
                subjects,
                new ArrayList<String>(),        // relatedResources
                new java.util.ArrayList<String>(),  //ApplacableLegislation
                null,						  // inSeries
                null,                           // qualifiedRelation
                "",							// temporalResolution
                new ArrayList<String>(),         // wasGeneratedBy
                new ArrayList<String>()          // HVDCategory
        );
    }

    private DcatDistribution buildJsonDistribution(String nbsId, String apiBaseUrl ) {
        String accessUrl = joinUrl(apiBaseUrl, "/api/nbs/" + nbsId);

        DcatDistribution d = new DcatDistribution();
        d.setNodeId(nodeId);
        d.setAccessUrl(accessUrl);
        d.setDownloadUrl(accessUrl);
        d.setTitle("nbs-json");
        d.setFormat("json");
        d.setMediaType("application/json");
        d.setReleaseDate("1970-01-01T00:00:00Z");
        d.setUpdateDate("1970-01-01T00:00:00Z");
        d.setDescription("NBS Registry detail endpoint (JSON)");
        return d;
    }

    private DcatDistribution buildLinkDistribution(String accessUrl, String title, String mediaType) {
        DcatDistribution d = new DcatDistribution();
        d.setNodeId(nodeId);
        d.setAccessUrl(accessUrl);
        d.setDownloadUrl(accessUrl);
        d.setTitle(title);
        if (StringUtils.isNotBlank(mediaType)) d.setMediaType(mediaType);
        d.setReleaseDate("1970-01-01T00:00:00Z");
        d.setUpdateDate("1970-01-01T00:00:00Z");
        d.setDescription(title != null ? title : "Related resource");
        return d;
    }

    /**
     * Landing page:
     * You provided the pattern:
     *   node.homepage + "/nbs-details/{id}"
     *
     * Example:
     *   homepage = https://dashboard-dev.urbreath.tech/tools/nbs-registry
     *   id=78 -> https://dashboard-dev.urbreath.tech/tools/nbs-registry/nbs-details/78
     */
    private String buildLandingPage(NbsData nbs) {
        if (nbs == null || nbs.id == null) return baseUrl();

        String homepage = node.getHomepage(); // existing field in OdmsCatalogue
        if (StringUtils.isNotBlank(homepage)) {
            return joinUrl(homepage, "/nbs-details/" + nbs.id);
        }

        // fallback
        return joinUrl(baseUrl(), "/api/nbs/" + nbs.id);
    }

    /* =====================
     * HTTP + Auth
     * ===================== */

    private String authenticate(NbsRegistryConnectorUtils.NbsConfig cfg) throws Exception {
    	String email = cfg.auth.email;
        String password = cfg.auth.password;

        if (StringUtils.isBlank(email) || StringUtils.isBlank(password)) {
            throw new Exception("Missing Urbreath credentials (email/password) in OdmsCatalogue for NBSRegistry.");
        }

        String authUrl = "https://services-dev.urbreath.tech/api/users/authenticate";
        
        //logger 800A
        logger.info("[NBS] Authenticating at {}", authUrl);
        
        Map<String, String> body = new HashMap<>();
        body.put("email", email);
        body.put("password", password);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(authUrl))
                .timeout(READ_TIMEOUT)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(body)))
                .build();
        
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        
        //logger 800A        
        logger.info("[NBS] Auth response: status={}, contentType={}",
                resp.statusCode(),
                resp.headers().firstValue("content-type").orElse(""));
        String authPreview = resp.body() != null ? resp.body() : "";
        authPreview = authPreview.length() > 400 ? authPreview.substring(0, 400) + "..." : authPreview;
        logger.debug("[NBS] Auth body preview: {}", authPreview);

        
        handleHttpErrors(resp);

        // Token extraction: supports common shapes
        Type mapType = new TypeToken<Map<String, Object>>(){}.getType();
        Map<String, Object> parsed = gson.fromJson(resp.body(), mapType);

        String token = extractToken(parsed);
        
        //logger 800A
        logger.info("[NBS] Auth token extracted? {} (len={})",
                StringUtils.isNotBlank(token),
                token != null ? token.length() : 0);

        
        if (StringUtils.isBlank(token)) {
            throw new Exception("Authenticate succeeded but token was not found in response payload.");
        }
        return token;
    }

    private BriefResponse fetchBriefPage(String token, NbsRegistryConnectorUtils.NbsConfig cfg, int page) throws Exception {
    	String url = joinUrl(
    			  apiBaseUrl(cfg),
    			  "/api/nbs/brief?page=" + page +
    			  "&size=" + cfg.paging.pageSize +
    			  "&sort=" + cfg.paging.sort +
    			  "&direction=" + cfg.paging.direction
    			);
    	
    	//logger 800A
    	logger.info("[NBS] Brief request: page={}, url={}", page, url);

    	
        BriefFilterBody body = new BriefFilterBody();
        
        if ("PILOT".equalsIgnoreCase(cfg.filter.mode)) {
            body.pilotIds = cfg.filter.ids;
            body.climateZoneIds = Collections.emptyList();
        } else { // CLIMATE_ZONE
            body.climateZoneIds = cfg.filter.ids;
            body.pilotIds = Collections.emptyList();
        }

        body.keywordIds = Collections.emptyList();
        body.problemIds = Collections.emptyList();
        body.onlyUrbreathNbs = cfg.filter.onlyUrbreathNbs;
        body.languageCode = cfg.filter.languageCode;
        
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(READ_TIMEOUT)
                .header("Content-Type", "application/json")
                // You confirmed bearer is required for brief:
                .header("Authorization", "Bearer " + token)
                .POST(HttpRequest.BodyPublishers.ofString(gson.toJson(body)))
                .build();
        
        //logger 800A
        logger.debug("[NBS] Brief filter body: {}", gson.toJson(body));

        
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        
        //logger 800A
        logger.info("[NBS] Brief response: status={}, contentType={}",
                resp.statusCode(),
                resp.headers().firstValue("content-type").orElse(""));
        String briefPreview = resp.body() != null ? resp.body() : "";
        briefPreview = briefPreview.length() > 400 ? briefPreview.substring(0, 400) + "..." : briefPreview;
        logger.debug("[NBS] Brief body preview: {}", briefPreview);

        
        handleHttpErrors(resp);

        //return gson.fromJson(resp.body(), BriefResponse.class);
        //logger 800A
        BriefResponse parsed = gson.fromJson(resp.body(), BriefResponse.class);
        int count = (parsed != null && parsed.data != null && parsed.data.results != null) ? parsed.data.results.size() : 0;
        logger.info("[NBS] Brief parsed: results={}, lastPage={}", count,
                parsed != null && parsed.data != null ? parsed.data.lastPage : null);
        return parsed;

    }

    private NbsDetailResponse fetchDetail(String token, String id, NbsRegistryConnectorUtils.NbsConfig cfg) throws Exception {
        String url = joinUrl(apiBaseUrl(cfg), "/api/nbs/" + id);
        
        //logger 800A
        logger.info("[NBS] Detail request: id={}, url={}", id, url);
        
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(READ_TIMEOUT)
                // Keeping bearer on detail too (safe default). Remove if not required.
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

        //logger 800A
        logger.info("[NBS] Detail response: id={}, status={}, contentType={}",
                id, resp.statusCode(),
                resp.headers().firstValue("content-type").orElse(""));
        String detPreview = resp.body() != null ? resp.body() : "";
        detPreview = detPreview.length() > 300 ? detPreview.substring(0, 300) + "..." : detPreview;
        logger.debug("[NBS] Detail body preview (id={}): {}", id, detPreview);

        
        handleHttpErrors(resp);

        String body = resp.body();
        String normalized = NbsRegistryConnectorUtils.normalizeKeywordsInJson(body);
        return gson.fromJson(normalized, NbsDetailResponse.class);
    }

    private void handleHttpErrors(HttpResponse<String> resp) throws Exception {
        int code = resp.statusCode();
        if (code >= 200 && code < 300) return;

        String ct = resp.headers().firstValue("content-type").orElse("");
        String body = resp.body() != null ? resp.body() : "";
        String preview = body.length() > 500 ? body.substring(0, 500) + "..." : body;
        String location = resp.headers().firstValue("location").orElse("");
        logger.error("[NBS] HTTP error: status={}, location={}, contentType={}, bodyPreview={}",
                code, location, ct, preview);


        String msg = "HTTP " + code + " - " + preview;
        if (code == 404) throw new OdmsCatalogueNotFoundException(msg);
        if (code == 403 || code == 401) throw new OdmsCatalogueForbiddenException(msg);
        throw new OdmsCatalogueOfflineException(msg);
    }


    /* =====================
     * Helpers
     * ===================== */

    private String baseUrl() {
        return (node.getHost() != null) ? node.getHost().trim().replaceAll("/+$", "") : "";
    }

    private String joinUrl(String base, String path) {
        if (StringUtils.isBlank(base)) return path == null ? "" : path;
        if (StringUtils.isBlank(path)) return base;

        // If already absolute, return as is
        if (path.startsWith("http://") || path.startsWith("https://")) return path;

        String b = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        String p = path.startsWith("/") ? path : "/" + path;
        return b + p;
    }

    private String safe(String s, String fallback) {
        return StringUtils.isNotBlank(s) ? s : fallback;
    }

    private List<DcatDataset> dedupByIdentifier(List<DcatDataset> in) {
        LinkedHashMap<String, DcatDataset> map = new LinkedHashMap<>();
        for (DcatDataset d : in) {
            if (d == null || d.getIdentifier() == null) continue;
            String id = String.valueOf(d.getIdentifier()).trim();
            if (id.isEmpty()) continue;
            map.putIfAbsent(id, d);
        }
        return new ArrayList<>(map.values());
    }

    @SuppressWarnings("unchecked")
    private String extractToken(Map<String, Object> parsed) {
        if (parsed == null) return null;

        Object direct = parsed.get("token");
        if (direct instanceof String) return (String) direct;

        Object accessToken = parsed.get("accessToken");
        if (accessToken instanceof String) return (String) accessToken;

        Object data = parsed.get("data");
        if (data instanceof Map) {
            Object t = ((Map<String, Object>) data).get("token");
            if (t instanceof String) return (String) t;

            Object at = ((Map<String, Object>) data).get("accessToken");
            if (at instanceof String) return (String) at;
        }

        return null;
    }
    
    // nbs url farlocco
    private String apiBaseUrl(NbsRegistryConnectorUtils.NbsConfig cfg) {
        String b = (cfg != null && StringUtils.isNotBlank(cfg.apiBaseUrl))
                ? cfg.apiBaseUrl.trim()
                : (node.getHost() != null ? node.getHost().trim() : "");
        return b.replaceAll("/+$", "");
    }
    
    private String buildProxiedImageUrl(String originalUrl, String proxyBaseUrl) {
        if (StringUtils.isBlank(originalUrl)) return null;

        int qIndex = originalUrl.indexOf('?');
        String cleanUrl = (qIndex > 0) ? originalUrl.substring(0, qIndex) : originalUrl;

        if (StringUtils.isBlank(proxyBaseUrl)) {
            return cleanUrl;
        }

        String proxy = proxyBaseUrl.endsWith("/")
                ? proxyBaseUrl.substring(0, proxyBaseUrl.length() - 1)
                : proxyBaseUrl;

        return proxy + "/" + cleanUrl;
    }
    
    private String detectImageMediaType(String url) {
        if (url == null) return "image/jpeg";

        String lower = url.toLowerCase();
        if (lower.endsWith(".png")) return "image/png";
        if (lower.endsWith(".webp")) return "image/webp";
        if (lower.endsWith(".jpeg")) return "image/jpeg";
        if (lower.endsWith(".jpg")) return "image/jpeg";
        return "image/jpeg";
    }




    /* =====================
     * Internal models (aligned to your API payloads)
     * ===================== */

    static class BriefFilterBody {
        List<Integer> climateZoneIds = new ArrayList<>();
        List<Integer> pilotIds = new ArrayList<>();
        List<Integer> keywordIds = new ArrayList<>();
        List<Integer> problemIds = new ArrayList<>();
        Boolean onlyUrbreathNbs = null;
        String languageCode = null;
    }

    // Matches your brief response
    static class BriefResponse {
        BriefData data;
        Object errors;
        String message;
        Boolean success;
        String timestamp;
    }

    static class BriefData {
        List<NbsBriefItem> results;
        Integer totalPages;
        Integer totalElements;
        Boolean lastPage;
    }

    static class NbsBriefItem {
        Integer id;
        String climateZone;
        String title;
        String pilot;
        // mainImage, objective, geoLocation exist but are ignored here
        Long dateCreated;
    }

    // Matches your detail response wrapper
    static class NbsDetailResponse {
        NbsData data;
        Object errors;
        String message;
        Boolean success;
        String timestamp;
    }

    public static class NbsData {
        public Integer id;
        public String climateZone;
        public String title;
        public String pilot;

        public GeoLocation geoLocation;

        public Boolean isUrbreathNbs;

        public List<String> keywords;

        public String objective;
        public String areaCharacterization;
        public Long dateCreated;
        public String status;
        public List<String> relatedMaterial;

        public String challenges;
        public String potentialImpactsAndBenefits;
        public String lessonsLearnt;

        public List<String> problems;

        // Images ignored for now
        public String mainImage;
        public List<ImageItem> images;

        // Videos
        public List<String> videoUrls;

        static class GeoLocation {
            Double latitude;
            Double longitude;
            String address;
        }

        static class ImageItem {
            Integer id;
            String url;
        }
    }
}
