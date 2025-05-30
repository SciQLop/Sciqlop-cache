CREATE TABLE IF NOT EXISTS `cache` (
    `id` INT unsigned NOT NULL AUTO_INCREMENT,
    `key` TEXT PRIMARY KEY NOT NULL COMMENT "The key is the path to the cache item from .cache/",
    `value` BLOB NOT NULL,
    `last_update` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `last_month_request_count` INT unsigned NOT NULL DEFAULT 0,
    `this_month_request_count` INT unsigned NOT NULL DEFAULT 0,
);
