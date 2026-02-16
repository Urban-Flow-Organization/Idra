package it.eng.idra.utils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

import it.eng.idra.connectors.NbsRegistryConnector;

/**
 * Utils for NBS Registry connector:
 * - HTML -> text
 * - epoch millis -> ISO 8601 Z
 * - building long description (Objective/Challenges/Problems/...)
 * - safe list helpers
 */
public final class NbsRegistryConnectorUtils {

    private NbsRegistryConnectorUtils() {}

    private static final DateTimeFormatter ISO_Z =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
                    .withZone(ZoneOffset.UTC);

    public static String epochMillisToIsoZ(Long epochMillis) {
        if (epochMillis == null || epochMillis <= 0) {
            return "1970-01-01T00:00:00Z";
        }
        return ISO_Z.format(Instant.ofEpochMilli(epochMillis));
    }

    /**
     * LONGTEXT description: concatenated labeled blocks.
     */
    public static String buildLongDescription(NbsRegistryConnector.NbsData nbs) {
        StringBuilder sb = new StringBuilder();

        appendSection(sb, "Objective", htmlToText(nbs.objective));
        appendSection(sb, "Challenges", htmlToText(nbs.challenges));

        if (nbs.problems != null && !nbs.problems.isEmpty()) {
            sb.append("Problems:\n");
            for (String p : nbs.problems) {
                String t = safeOneLine(p);
                if (!t.isBlank()) sb.append("- ").append(t).append("\n");
            }
            sb.append("\n");
        }

        appendSection(sb, "Potential impacts and benefits", htmlToText(nbs.potentialImpactsAndBenefits));
        appendSection(sb, "Lessons learnt", htmlToText(nbs.lessonsLearnt));
        appendSection(sb, "Area characterization", htmlToText(nbs.areaCharacterization));

        return sb.toString().trim();
    }

    private static void appendSection(StringBuilder sb, String title, String content) {
        if (content == null) return;
        String c = content.trim();
        if (c.isEmpty()) return;
        sb.append(title).append(":\n");
        sb.append(c).append("\n\n");
    }

    /**
     * Lightweight HTML -> text conversion (no external libs).
     */
    public static String htmlToText(String html) {
        if (html == null) return "";
        String s = html;

        s = s.replaceAll("(?i)<br\\s*/?>", "\n");
        s = s.replaceAll("(?i)</p\\s*>", "\n\n");
        s = s.replaceAll("(?i)</div\\s*>", "\n");
        s = s.replaceAll("(?i)</li\\s*>", "\n");
        s = s.replaceAll("(?i)<li\\s*>", "- ");

        s = s.replaceAll("<[^>]+>", "");

        s = unescapeBasicEntities(s);
        s = unescapeNumericEntities(s);

        s = s.replace("\r", "");
        s = s.replaceAll("[ \t]+", " ");
        s = s.replaceAll("\\n{3,}", "\n\n");
        return s.trim();
    }

    private static String unescapeBasicEntities(String s) {
        return s
                .replace("&amp;", "&")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&quot;", "\"")
                .replace("&#39;", "'")
                .replace("&nbsp;", " ");
    }

    private static String unescapeNumericEntities(String s) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < s.length()) {
            int start = s.indexOf("&#", i);
            if (start < 0) {
                out.append(s.substring(i));
                break;
            }
            out.append(s, i, start);
            int end = s.indexOf(";", start);
            if (end < 0) {
                out.append(s.substring(start));
                break;
            }
            String ent = s.substring(start + 2, end);
            try {
                int codepoint;
                if (ent.startsWith("x") || ent.startsWith("X")) {
                    codepoint = Integer.parseInt(ent.substring(1), 16);
                } else {
                    codepoint = Integer.parseInt(ent, 10);
                }
                out.appendCodePoint(codepoint);
            } catch (Exception e) {
                out.append(s, start, end + 1);
            }
            i = end + 1;
        }
        return out.toString();
    }

    public static List<String> dedupNonEmpty(List<String> in) {
        if (in == null) return Collections.emptyList();
        LinkedHashSet<String> set = new LinkedHashSet<>();
        for (String s : in) {
            if (s == null) continue;
            String t = s.trim();
            if (!t.isEmpty()) set.add(t);
        }
        return new ArrayList<>(set);
    }

    private static String safeOneLine(String s) {
        if (s == null) return "";
        return s.replace("\n", " ").replace("\r", " ").trim();
    }
    
 // ---- NBS Registry config parsing (from OdmsCatalogue.connectorParams) ----

    public static final class NbsConfig {
      public String type;        // "NBS_REGISTRY"
      public String apiBaseUrl;     //Test Url farlocco
      public Auth auth;
      public Filter filter;
      public Paging paging;
      public String proxyBaseUrl; // e.g. https://my-proxy-domain


      public static final class Auth {
        public String email;
        public String password;
      }

      public static final class Filter {
        public String mode;          // "PILOT" | "CLIMATE_ZONE"
        public List<Integer> ids;    // selected IDs
        public Boolean onlyUrbreathNbs;
        public String languageCode;
      }

      public static final class Paging {
        public Integer pageSize;
        public String sort;
        public String direction;
      }
    }

    public static NbsConfig readNbsConfig(String connectorParamsJson) {
      if (connectorParamsJson == null || connectorParamsJson.trim().isEmpty()) {
        throw new IllegalArgumentException("Missing connectorParams: please configure NBS Registry parameters (auth + filter).");
      }

      final com.google.gson.Gson gson = new com.google.gson.Gson();
      final NbsConfig cfg;
      try {
        cfg = gson.fromJson(connectorParamsJson, NbsConfig.class);
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid connectorParams JSON: " + e.getMessage(), e);
      }

      // defaults + validation
      if (cfg == null) {
        throw new IllegalArgumentException("Invalid connectorParams JSON: parsed as null.");
      }

      if (cfg.type == null || !cfg.type.trim().equalsIgnoreCase("NBS_REGISTRY")) {
        // keep it strict to avoid silent misconfigurations
        throw new IllegalArgumentException("connectorParams.type must be 'NBS_REGISTRY'.");
      }

      if (cfg.auth == null || isBlank(cfg.auth.email) || isBlank(cfg.auth.password)) {
        throw new IllegalArgumentException("Missing auth.email/auth.password in connectorParams.");
      }
      if (isBlank(cfg.apiBaseUrl)) cfg.apiBaseUrl = null; // lo lasci null e fai fallback nel connector -- test url farlocco

      if (cfg.filter == null) cfg.filter = new NbsConfig.Filter();
      if (cfg.filter.mode == null) cfg.filter.mode = "PILOT";
      if (cfg.filter.ids == null) cfg.filter.ids = new java.util.ArrayList<>();

      if (cfg.filter.onlyUrbreathNbs == null) cfg.filter.onlyUrbreathNbs = Boolean.FALSE;
      if (cfg.filter.languageCode == null) cfg.filter.languageCode = "en";

      if (cfg.paging == null) cfg.paging = new NbsConfig.Paging();
      if (cfg.paging.pageSize == null || cfg.paging.pageSize <= 0) cfg.paging.pageSize = 50;
      if (isBlank(cfg.paging.sort)) cfg.paging.sort = "title";
      if (isBlank(cfg.paging.direction)) cfg.paging.direction = "asc";
      if (cfg.proxyBaseUrl == null) cfg.proxyBaseUrl = "";

      // normalize mode
      cfg.filter.mode = cfg.filter.mode.trim().toUpperCase();
      if (!cfg.filter.mode.equals("PILOT") && !cfg.filter.mode.equals("CLIMATE_ZONE")) {
        throw new IllegalArgumentException("filter.mode must be 'PILOT' or 'CLIMATE_ZONE'.");
      }

      return cfg;
    }

    private static boolean isBlank(String s) {
      return s == null || s.trim().isEmpty();
    }

}
