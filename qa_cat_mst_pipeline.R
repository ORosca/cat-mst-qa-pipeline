# qa_cat_pipeline.R
# ============================================================
# Reproducible QA pipeline for CAT/MST item response data
# Author: Oxana Rosca (adaptable across institutions/years)
# ============================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(readxl)
  library(lubridate)
  library(ggplot2)
  library(stringr)
  library(digest)
})

# ---------------------------
# Helpers
# ---------------------------

assert_has_cols <- function(df, cols, df_name = "data") {
  missing <- setdiff(cols, names(df))
  if (length(missing) > 0) {
    stop(sprintf("[%s] missing required columns: %s", df_name, paste(missing, collapse = ", ")), call. = FALSE)
  }
  invisible(TRUE)
}

as_chr_if_needed <- function(x) {
  if (is.character(x)) return(x)
  as.character(x)
}

parse_datetime_safe <- function(x, tz = "UTC") {
  # Handles POSIXct already, and common string formats.
  if (inherits(x, "POSIXct")) return(x)
  if (is.numeric(x)) return(as.POSIXct(x, origin = "1970-01-01", tz = tz)) # if epoch
  x <- as.character(x)
  parsed <- suppressWarnings(ymd_hms(x, tz = tz))
  if (all(is.na(parsed))) parsed <- suppressWarnings(mdy_hms(x, tz = tz))
  if (all(is.na(parsed))) parsed <- suppressWarnings(ymd_hm(x, tz = tz))
  parsed
}

`%||%` <- function(a, b) if (!is.null(a)) a else b

normalize_daacs_id <- function(x) {
  # Preserve intended ID string when it arrives as character (keeps leading zeros).
  # If numeric, leading zeros cannot be recovered; convert to clean integer-like string.
  if (is.character(x)) {
    y <- trimws(x)
    y[y == ""] <- NA_character_
    return(y)
  }
  y <- as.character(x)
  y <- trimws(y)
  y[y == ""] <- NA_character_
  y <- sub("\\.0+$", "", y)  # remove trailing ".0" when numeric was coerced
  y
}

make_global_id <- function(college_id, wave, daacs_id_raw) {
  key <- paste(college_id, as.character(wave), daacs_id_raw, sep = "|")
  vapply(key, digest, character(1), algo = "xxhash64")
}

add_global_ids <- function(df, college_id, wave, id_col = "DAACS_ID") {
  if (!id_col %in% names(df)) stop("Missing ID column: ", id_col, call. = FALSE)

  df %>%
    mutate(
      college_id = college_id,
      wave = as.character(wave),
      DAACS_ID_raw = normalize_daacs_id(.data[[id_col]]),
      DAACS_ID_global = make_global_id(college_id, wave, DAACS_ID_raw)
    )
}

assert_unique_id <- function(df, id = "DAACS_ID_global", df_name = "data") {
  n_dupes <- sum(duplicated(df[[id]]))
  if (n_dupes > 0) stop(sprintf("[%s] %s has %d duplicates", df_name, id, n_dupes), call. = FALSE)
  invisible(TRUE)
}

# ---------------------------
# 1) Load + standardize
# ---------------------------

load_inputs <- function(cfg) {
  student <- readRDS(cfg$paths$student_rds)
  items   <- readRDS(cfg$paths$item_rds)

  if (!is.null(cfg$paths$mapping_xlsx)) {
    mapping <- read_excel(cfg$paths$mapping_xlsx, sheet = cfg$paths$mapping_sheet %||% 1)
  } else {
    mapping <- NULL
  }

  list(student = student, items = items, mapping = mapping)
}

standardize_and_add_ids <- function(student, items, cfg) {
  # Add global IDs to both student and item data
  student <- add_global_ids(student, cfg$college_id, cfg$wave, id_col = cfg$cols$id)
  items   <- add_global_ids(items,   cfg$college_id, cfg$wave, id_col = cfg$cols$id)

  # Remove duplicate raw IDs in student data (keep first occurrence)
  raw_id_col <- cfg$cols$id
  if (sum(duplicated(student[[raw_id_col]])) > 0) {
    n_dupes <- sum(duplicated(student[[raw_id_col]]))
    warning(sprintf("[student] Removed %d duplicate(s) based on raw %s (kept first occurrence)", n_dupes, raw_id_col), call. = FALSE)
    student <- student[!duplicated(student[[raw_id_col]]), ]
  }

  # Sanity checks
  assert_unique_id(student, "DAACS_ID_global", "student")
  # items will have repeated DAACS_ID_global across rows (multiple item rows per student),
  # so uniqueness does NOT apply there.

  list(student = student, items = items)
}


# ---------------------------
# 2) Map QIDs
# ---------------------------

map_qids <- function(items, mapping, cfg) {
  # If no mapping provided, assume items already contain cfg$cols$qid_final
  qid_source <- cfg$cols$qid_source
  qid_final  <- cfg$cols$qid_final

  assert_has_cols(items, c(cfg$cols$id, qid_source, cfg$cols$score), "items")

  if (is.null(mapping)) {
    if (!qid_final %in% names(items)) {
      # If final doesn't exist, treat source as final
      items[[qid_final]] <- items[[qid_source]]
    }
    return(items)
  }

  assert_has_cols(mapping, c(cfg$mapping$qid_source_in_map, cfg$mapping$qid_final_in_map), "mapping")

  items[[qid_final]] <- mapping[[cfg$mapping$qid_final_in_map]][
    match(items[[qid_source]], mapping[[cfg$mapping$qid_source_in_map]])
  ]

  # Flag unmapped
  unmapped <- unique(items[[qid_source]][is.na(items[[qid_final]])])
  if (length(unmapped) > 0) {
    warning(sprintf("[%s] Unmapped qids found: %s", cfg$name, paste(unmapped, collapse = ", ")), call. = FALSE)
  }

  items
}

# ---------------------------
# 3) Keep first attempt + pivot wide
# ---------------------------

keep_first_attempt <- function(items, cfg) {
  if (!is.null(cfg$cols$attempt) && cfg$cols$attempt %in% names(items)) {
    items <- items %>% filter(.data[[cfg$cols$attempt]] == cfg$filters$attempt_value)
  }
  items
}

pivot_items_wide <- function(items, cfg) {
  id  <- "DAACS_ID_global"
  qid <- cfg$cols$qid_final
  sc  <- cfg$cols$score

  items_long <- items %>% select(all_of(c(id, qid, sc)))

  items_wide <- items_long %>%
    pivot_wider(names_from = all_of(qid), values_from = all_of(sc))

  items_wide
}

# ---------------------------
# 4) Merge time, filter rapid & low effort
# ---------------------------

add_time_taken <- function(items_wide, student, cfg) {
  if (is.null(cfg$time$start) || is.null(cfg$time$end)) return(items_wide)

  assert_has_cols(student, c(cfg$cols$id, cfg$time$start, cfg$time$end), "student")

  student <- student %>%
    mutate(
      start_dt = parse_datetime_safe(.data[[cfg$time$start]], tz = cfg$time$tz %||% "UTC"),
      end_dt   = parse_datetime_safe(.data[[cfg$time$end]],   tz = cfg$time$tz %||% "UTC"),
      time_min = as.numeric(difftime(end_dt, start_dt, units = "mins"))
    ) %>%
    select(DAACS_ID_global, time_min)

  items_wide %>%
    left_join(student, by = "DAACS_ID_global")
}

filter_time_and_min_items <- function(df, cfg) {
  id_col <- "DAACS_ID_global"
  non_item_cols <- c(id_col, "time_min")

  item_cols <- setdiff(names(df), non_item_cols)
  df <- df %>%
    mutate(non_missing_count = rowSums(!is.na(across(all_of(item_cols)))))

  # Time filter (if present)
  if ("time_min" %in% names(df) && !is.null(cfg$filters$min_time_min)) {
    df <- df %>% filter(is.na(time_min) | time_min >= cfg$filters$min_time_min)
  }

  # Minimum items filter
  if (!is.null(cfg$filters$min_items)) {
    df <- df %>% filter(non_missing_count >= cfg$filters$min_items)
  }

  df
}

# ---------------------------
# 5) QA checks (return results, don’t just print)
# ---------------------------

qa_duplicates <- function(student, items_wide, cfg) {
  id <- cfg$cols$id
  list(
    student_dupes_n = sum(duplicated(student[[id]])),
    items_dupes_n   = sum(duplicated(items_wide[["DAACS_ID_global"]]))
  )
}

qa_identical_response_patterns <- function(items_wide, cfg) {
  mat <- items_wide %>% select(-"DAACS_ID_global") %>% as.data.frame()

  dup_any <- duplicated(mat) | duplicated(mat, fromLast = TRUE)
  ids <- items_wide[["DAACS_ID_global"]][dup_any]

  list(
    identical_patterns_n = sum(dup_any),
    identical_ids = sort(unique(ids))
  )
}

qa_non_discriminative_items <- function(df, cfg) {
  ignore <- c("DAACS_ID_global", "time_min", "non_missing_count")
  item_cols <- setdiff(names(df), ignore)

  non_disc <- item_cols[sapply(df[item_cols], function(x) {
    ux <- unique(x[!is.na(x)])
    length(ux) <= 1
  })]

  non_disc
}

qa_value_ranges <- function(df, cfg) {
  ignore <- c("DAACS_ID_global", "time_min", "non_missing_count")
  item_cols <- setdiff(names(df), ignore)

  tibble(
    QID = item_cols,
    min = sapply(df[item_cols], function(x) suppressWarnings(min(x, na.rm = TRUE))),
    max = sapply(df[item_cols], function(x) suppressWarnings(max(x, na.rm = TRUE))),
    n   = sapply(df[item_cols], function(x) sum(!is.na(x)))
  ) %>% arrange(QID)
}

qa_response_counts_by_difficulty <- function(item_table) {
  # expects item_table has QID and Count
  item_table %>%
    mutate(difficulty = str_extract(QID, "[EMH](?!.*[EMH])")) %>%  # last E/M/H in string
    filter(difficulty %in% c("E","M","H")) %>%
    group_by(difficulty) %>%
    summarise(
      items = n(),
      min_count = min(Count),
      median_count = median(Count),
      .groups = "drop"
    )
}

plot_response_count_density <- function(item_table, out_file, title = "Item response count distribution") {
  item_table2 <- item_table %>%
    mutate(difficulty = str_extract(QID, "[EMH](?!.*[EMH])")) %>%
    filter(difficulty %in% c("E","M","H"))

  min_count <- min(item_table2$Count)

  p <- ggplot(item_table2, aes(x = Count, fill = difficulty)) +
    geom_density(alpha = 0.35) +
    geom_vline(xintercept = min_count, linetype = "dashed") +
    labs(title = title,
         subtitle = paste("Min item response count:", min_count),
         x = "Response Count", y = "Density", fill = "Difficulty") +
    theme_minimal()

  ggsave(out_file, p, width = 10, height = 7, dpi = 150)
  invisible(p)
}

# ---------------------------
# 6) One runner function per dataset
# ---------------------------

run_qa_pipeline <- function(cfg, write_outputs = TRUE) {
  message("==== Running QA for: ", cfg$name, " ====")

  inp <- load_inputs(cfg)
  std <- standardize_and_add_ids(inp$student, inp$items, cfg)

  items <- map_qids(std$items, inp$mapping, cfg)
  items <- keep_first_attempt(items, cfg)

  wide <- pivot_items_wide(items, cfg)
  wide <- add_time_taken(wide, std$student, cfg)
  wide <- filter_time_and_min_items(wide, cfg)

  # --- QA outputs ---
  dupes <- qa_duplicates(std$student, wide, cfg)
  ident <- qa_identical_response_patterns(wide, cfg)
  nondisc <- qa_non_discriminative_items(wide, cfg)
  ranges <- qa_value_ranges(wide, cfg)

  # item table
  item_cols <- setdiff(names(wide), c("DAACS_ID_global", "time_min", "non_missing_count"))
  item_table <- tibble(
    QID = item_cols,
    Count = sapply(wide[item_cols], function(x) sum(!is.na(x)))
  ) %>% arrange(QID)

  diff_summary <- qa_response_counts_by_difficulty(item_table)

  out <- list(
    meta = cfg[c("name","college_id","wave","version","date")],
    n_students = nrow(wide),
    n_items = length(item_cols),
    duplicates = dupes,
    identical_patterns = ident,
    non_discriminative_items = nondisc,
    item_ranges = ranges,
    item_response_counts = item_table,
    response_counts_by_difficulty = diff_summary
  )

  if (write_outputs) {
    dir.create(cfg$output_dir, showWarnings = FALSE, recursive = TRUE)

    saveRDS(out, file.path(cfg$output_dir, paste0(cfg$name, "_qa_results.rds")))
    write.csv(ranges, file.path(cfg$output_dir, paste0(cfg$name, "_item_ranges.csv")), row.names = FALSE)
    write.csv(item_table, file.path(cfg$output_dir, paste0(cfg$name, "_item_counts.csv")), row.names = FALSE)
    write.csv(diff_summary, file.path(cfg$output_dir, paste0(cfg$name, "_counts_by_difficulty.csv")), row.names = FALSE)

    plot_response_count_density(
      item_table,
      out_file = file.path(cfg$output_dir, paste0(cfg$name, "_density_counts.png")),
      title = paste0(cfg$name, ": Distribution of Item Response Counts")
    )
  }

  out
}

# ============================================================
# CONFIGS (edit these for each dataset)
# ============================================================

configs <- list(

  list(
    name      = "wgu_math_2017",
    college_id = "wgu",
    wave      = 2017,
    version   = "V1",
    date      = "2017-10-31",

    paths = list(
      student_rds = "D:/DData/daacs_wgu.rds",
      item_rds    = "D:/DData/math_items_wgu.rds",
      mapping_xlsx = "D:/DData/MappingQID_math_wgu.xlsx",
      mapping_sheet = 1
    ),

    mapping = list(
      qid_source_in_map = "qid_wgu",
      qid_final_in_map  = "QID"
    ),

    cols = list(
      id = "DAACS_ID",
      qid_source = "qid",
      qid_final  = "QID",
      score = "score",
      attempt = "attempt"
    ),

    filters = list(
      attempt_value = 1,
      min_time_min  = 3,
      min_items     = 18
    ),

    time = list(
      start = "mathStartDate",
      end   = "mathCompletionDate",
      tz    = "America/New_York"
    ),

    output_dir = "outputs/WGU_Math_2017_V1"
  ),
  list(
    name      = "umgc_math_2022",
    college_id = "umgc",
    wave      = 2022,
    version   = "V2",
    date      = "2023-05-18",

    paths = list(
      student_rds = "D:/DData/daacs_umgc1.rds",
      item_rds    = "D:/DData/math_items_umgc1.rds",
      mapping_xlsx = "D:/DData/MappingQID_math.xlsx",
      mapping_sheet = 1
    ),

    mapping = list(
      qid_source_in_map = "qid_umgc1",
      qid_final_in_map  = "QID"
    ),

    cols = list(
      id = "DAACS_ID",
      qid_source = "qid",
      qid_final  = "QID",
      score = "score",
      attempt = "attempt"
    ),

    filters = list(
      attempt_value = 1,
      min_time_min  = 3,
      min_items     = 18
    ),

    time = list(
      start = "mathStartDate",
      end   = "mathCompletionDate",
      tz    = "America/New_York"
    ),

    output_dir = "outputs/UMGC_Math_2022_V2"
  ),
  list(
    name      = "ua_math_2022",
    college_id = "ua",
    wave      = 2022,
    version   = "V2",
    date      = "2023-04-11",

    paths = list(
      student_rds = "D:/DData/daacs_ua2.rds",
      item_rds    = "D:/DData/math_items_ua2.rds",
      mapping_xlsx = "D:/DData/MappingQID_math.xlsx",
      mapping_sheet = 1
    ),

    mapping = list(
      qid_source_in_map = "qid_ua2",
      qid_final_in_map  = "QID"
    ),

    cols = list(
      id = "DAACS_ID",
      qid_source = "qid",
      qid_final  = "QID",
      score = "score",
      attempt = "attempt"
    ),

    filters = list(
      attempt_value = 1,
      min_time_min  = 3,
      min_items     = 18
    ),

    time = list(
      start = "mathStartDate",
      end   = "mathCompletionDate",
      tz    = "America/New_York"
    ),

    output_dir = "outputs/UA_Math_2022_V2"
  )
  # Add UMGC and UA2 configs here with their file paths and (if needed) mapping rules
)

# ============================================================
# Run all
# ============================================================

results <- lapply(configs, run_qa_pipeline, write_outputs = TRUE)
