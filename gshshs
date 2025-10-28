package com.example.fpm.service;

import com.example.fpm.model.PathConfig;
import com.example.fpm.model.RoutingLog;
import com.example.fpm.repository.PathConfigRepository;
import com.example.fpm.repository.RoutingLogRepository;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;
import com.example.fpm.sharepoint.SharePointService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Locale;
import java.io.IOException;
import java.nio.file.*;
import java.util.stream.Collectors;

@Service
public class RoutingService {
    private final PathConfigRepository pathConfigRepository;
    private final RoutingLogRepository routingLogRepository;
    private final SharePointService sharePointService;
    @Value("${app.routing.mode:dry-run}")
    private String routingMode;
    @Value("${app.storage.local.baseDir:}")
    private String localBaseDir;

    public RoutingService(PathConfigRepository pathConfigRepository, RoutingLogRepository routingLogRepository, SharePointService sharePointService) {
        this.pathConfigRepository = pathConfigRepository;
        this.routingLogRepository = routingLogRepository;
        this.sharePointService = sharePointService;
    }

    // Stub: later integrate with Microsoft Graph to process incoming/ folder
    public Map<String, Object> runRoutingNow() {
        if ("local".equalsIgnoreCase(routingMode)) {
            return runLocalRouting();
        } else if ("live".equalsIgnoreCase(routingMode)) {
            List<PathConfig> configs = pathConfigRepository.findAll();
            return sharePointService.runLiveRouting(configs, this::persistLog);
        }
        Map<String, Object> summary = new HashMap<>();
        summary.put("processed", 0);
        summary.put("moved", 0);
        summary.put("skipped", 0);
        summary.put("errors", 0);
        return summary;
    }

    private Map<String, Object> routeSingleLocal(String fileName) {
        Map<String, Object> res = new HashMap<>();
        if (localBaseDir == null || localBaseDir.trim().isEmpty()) {
            res.put("moved", false);
            res.put("reason", "baseDir not configured");
            return res;
        }
        Path base = Paths.get(localBaseDir);
        Path incoming = base.resolve("incoming");
        Path source = incoming.resolve(fileName);
        if (!Files.exists(source)) {
            res.put("moved", false);
            res.put("reason", "file not found in incoming");
            return res;
        }
        int sep = fileName.indexOf('_');
        if (sep <= 0) {
            persistLog(fileName, "SKIPPED", incoming.toString(), "", "Missing prefix delimiter '_'");
            res.put("moved", false);
            res.put("reason", "missing prefix delimiter '_'");
            return res;
        }
        String prefix = fileName.substring(0, sep);
        String outRel = detectOutputBase(prefix); // group-based mapping e.g., reports/Finance
        Path outDir = base.resolve(outRel.replace("/", java.io.File.separator));
        try {
            if (!Files.exists(outDir)) Files.createDirectories(outDir);
            Path target = outDir.resolve(fileName);
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
            persistLog(fileName, "MOVED", incoming.toString(), target.toString(), null);
            res.put("moved", true);
            res.put("destination", target.toString());
            return res;
        } catch (IOException e) {
            persistLog(fileName, "ERROR", incoming.toString(), outDir.toString(), e.getMessage());
            res.put("moved", false);
            res.put("reason", e.getMessage());
            return res;
        }
    }

    public static class DryRunDecision {
        private String fileName;
        private String prefix;
        private boolean matched;
        private String outputPath; // reports/<Group>
        private String destinationPath; // outputPath + "/" + fileName
        private String reason; // if not matched

        public DryRunDecision() {}

        public DryRunDecision(String fileName, String prefix, boolean matched, String outputPath, String destinationPath, String reason) {
            this.fileName = fileName;
            this.prefix = prefix;
            this.matched = matched;
            this.outputPath = outputPath;
            this.destinationPath = destinationPath;
            this.reason = reason;
        }

        public String getFileName() { return fileName; }
        public void setFileName(String fileName) { this.fileName = fileName; }
        public String getPrefix() { return prefix; }
        public void setPrefix(String prefix) { this.prefix = prefix; }
        public boolean isMatched() { return matched; }
        public void setMatched(boolean matched) { this.matched = matched; }
        public String getOutputPath() { return outputPath; }
        public void setOutputPath(String outputPath) { this.outputPath = outputPath; }
        public String getDestinationPath() { return destinationPath; }
        public void setDestinationPath(String destinationPath) { this.destinationPath = destinationPath; }
        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }
    }

    public List<DryRunDecision> dryRunDecisions(List<String> fileNames) {
        List<PathConfig> configs = pathConfigRepository.findAll();
        List<DryRunDecision> decisions = new ArrayList<>();
        for (String name : fileNames) {
            if (name == null || name.trim().isEmpty()) continue;
            String trimmed = name.trim();
            int sep = trimmed.indexOf('_');
            if (sep <= 0) {
                decisions.add(new DryRunDecision(trimmed, null, false, null, null, "Filename missing prefix delimiter '_'"));
                continue;
            }
            String prefix = trimmed.substring(0, sep);
            String outputBase = detectOutputBase(prefix);
            String destination = (outputBase.endsWith("/")) ? outputBase + trimmed : outputBase + "/" + trimmed;
            decisions.add(new DryRunDecision(trimmed, prefix, true, outputBase, destination, null));
        }
        return decisions;
    }

    public List<Map<String, Object>> listIncomingFiles() {
        if (!"local".equalsIgnoreCase(routingMode)) {
            return List.of();
        }
        if (localBaseDir == null || localBaseDir.trim().isEmpty()) {
            return List.of();
        }
        Path incoming = Paths.get(localBaseDir).resolve("incoming");
        if (!Files.exists(incoming)) return List.of();
        try {
            return Files.list(incoming)
                    .filter(Files::isRegularFile)
                    .map(p -> {
                        Map<String, Object> m = new HashMap<>();
                        m.put("name", p.getFileName().toString());
                        try {
                            m.put("size", Files.size(p));
                            m.put("modified", Files.getLastModifiedTime(p).toMillis());
                        } catch (IOException ignored) {}
                        return m;
                    })
                    .collect(Collectors.toList());
        } catch (IOException e) {
            return List.of();
        }
    }

    public Map<String, Object> routeSingle(String fileName) {
        if (fileName == null || fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("fileName is required");
        }
        if ("local".equalsIgnoreCase(routingMode)) {
            return routeSingleLocal(fileName.trim());
        } else if ("live".equalsIgnoreCase(routingMode)) {
            // Optional: implement live single move later
            throw new UnsupportedOperationException("Single-file routing not implemented for live mode");
        }
        Map<String, Object> res = new HashMap<>();
        res.put("moved", false);
        res.put("reason", "routing disabled");
        return res;
    }

    private Map<String, Object> runLocalRouting() {
        Map<String, Object> summary = new HashMap<>();
        int processed = 0;
        int moved = 0;
        int skipped = 0;
        int errors = 0;
        if (localBaseDir == null || localBaseDir.trim().isEmpty()) {
            summary.put("processed", processed);
            summary.put("moved", moved);
            summary.put("skipped", skipped);
            summary.put("errors", errors);
            return summary;
        }
        Path base = Paths.get(localBaseDir);
        Path incoming = base.resolve("incoming");
        try {
            if (!Files.exists(incoming)) {
                Files.createDirectories(incoming);
            }
            List<Path> files = Files.list(incoming)
                    .filter(p -> Files.isRegularFile(p))
                    .collect(Collectors.toList());
            for (Path file : files) {
                processed++;
                String fileName = file.getFileName().toString();
                int sep = fileName.indexOf('_');
                if (sep <= 0) {
                    skipped++;
                    persistLog(fileName, "SKIPPED", incoming.toString(), "", "Missing prefix delimiter '_'");
                    continue;
                }
                String prefix = fileName.substring(0, sep);
                String outRel = detectOutputBase(prefix);
                Path outDir = base.resolve(outRel.replace("/", java.io.File.separator));
                if (!Files.exists(outDir)) {
                    Files.createDirectories(outDir);
                }
                Path target = outDir.resolve(fileName);
                try {
                    Files.move(file, target, StandardCopyOption.REPLACE_EXISTING);
                    moved++;
                    persistLog(fileName, "MOVED", incoming.toString(), target.toString(), null);
                } catch (IOException ex) {
                    errors++;
                    persistLog(fileName, "ERROR", incoming.toString(), outDir.toString(), ex.getMessage());
                }
            }
        } catch (IOException e) {
            // ignore here; counts remain
        }
        summary.put("processed", processed);
        summary.put("moved", moved);
        summary.put("skipped", skipped);
        summary.put("errors", errors);
        return summary;
    }

    private String detectOutputBase(String rawPrefix) {
        if (rawPrefix == null || rawPrefix.isEmpty()) return "reports/Unmapped";
        String p = rawPrefix.trim();
        String lower = p.toLowerCase(Locale.ROOT);
        // Known groups mapping
        if (lower.equals("finance")) return "reports/Finance";
        if (lower.equals("risk")) return "reports/Risk"; // Risk Management → Risk
        if (lower.equals("trading")) return "reports/Trading";
        if (lower.equals("hr")) return "reports/HR"; // HR Analytics → HR
        if (lower.equals("operations")) return "reports/Operations";
        if (lower.equals("compliance")) return "reports/Compliance";
        // fallback: capitalize first letter
        String cap = p.substring(0,1).toUpperCase(Locale.ROOT) + p.substring(1);
        return "reports/" + cap;
    }

    private void persistLog(String fileName, String action, String from, String to, String message) {
        RoutingLog log = new RoutingLog();
        log.setFileName(fileName);
        log.setAction(action);
        log.setFromPath(from);
        log.setToPath(to);
        log.setMessage(message);
        routingLogRepository.save(log);
    }
}
