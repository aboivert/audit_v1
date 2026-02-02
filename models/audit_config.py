from datetime import datetime
import uuid

from .project import db


class AuditConfig(db.Model):
    __tablename__ = 'audit_configs'
    
    # Colonnes principales
    config_id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id = db.Column(db.String(36), db.ForeignKey('projects.project_id'), nullable=False)
    date_configuration = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    
    # Colonnes de configuration (exemples - à adapter selon tes fonctions)
    conf_agency_completion_phone = db.Column(db.Boolean, default=False, nullable=False)
    conf_routes_validate_route_colors = db.Column(db.Boolean, default=False, nullable=False)

    conf_agency_completion_url = db.Column(db.Boolean, default=False, nullable=False)  # Complétude URL
    conf_agency_completion_phone = db.Column(db.Boolean, default=False, nullable=False)  # Complétude téléphone
    conf_agency_check_required_columns = db.Column(db.Boolean, default=False, nullable=False)  # check_required_columns
    conf_agency_validate_agency_url = db.Column(db.Boolean, default=False, nullable=False)  # validate_agency_url
    conf_agency_validate_agency_timezone = db.Column(db.Boolean, default=False, nullable=False)  # validate_agency_timezone
    conf_agency_check_duplicate_agency_id = db.Column(db.Boolean, default=False, nullable=False)  # check_duplicate_agency_id
    conf_agency_missing_values_stats = db.Column(db.Boolean, default=False, nullable=False)  # missing_values_stats
    conf_agency_check_field_length = db.Column(db.Boolean, default=False, nullable=False)  # check_field_length
    conf_agency_validate_agency_lang = db.Column(db.Boolean, default=False, nullable=False)  # validate_agency_lang
    conf_agency_validate_agency_phone = db.Column(db.Boolean, default=False, nullable=False)  # validate_agency_phone
    conf_agency_validate_agency_fare_url = db.Column(db.Boolean, default=False, nullable=False)  # validate_agency_fare_url
    conf_agency_check_agency_consistency = db.Column(db.Boolean, default=False, nullable=False)  # check_agency_consistency
    conf_agency_duplicate_agencies_by_name_contact = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_agencies_by_name_contact
    conf_calendar_invalid_or_inverted_dates = db.Column(db.Boolean, default=False, nullable=False)  # invalid_or_inverted_dates
    conf_calendar_inactive_services = db.Column(db.Boolean, default=False, nullable=False)  # inactive_services
    conf_calendar_excessive_duration_services = db.Column(db.Boolean, default=False, nullable=False)  # excessive_duration_services
    conf_calendar_calendar_dates_service_not_in_calendar = db.Column(db.Boolean, default=False, nullable=False)  # calendar_dates_service_not_in_calendar
    conf_calendar_exceptions_outside_date_range = db.Column(db.Boolean, default=False, nullable=False)  # exceptions_outside_date_range
    conf_calendar_never_active_services = db.Column(db.Boolean, default=False, nullable=False)  # never_active_services
    conf_calendar_duplicate_service_id_days = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_service_id_days
    conf_calendar_dates_invalid_dates = db.Column(db.Boolean, default=False, nullable=False)  # invalid_dates
    conf_calendar_dates_invalid_exception_types = db.Column(db.Boolean, default=False, nullable=False)  # invalid_exception_types
    conf_calendar_dates_duplicate_calendar_dates = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_calendar_dates
    conf_calendar_dates_conflicting_exceptions = db.Column(db.Boolean, default=False, nullable=False)  # conflicting_exceptions
    conf_calendar_dates_duplicate_calendar_dates = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_calendar_dates
    conf_calendar_dates_calendar_dates_conflicts_with_calendar = db.Column(db.Boolean, default=False, nullable=False)  # calendar_dates_conflicts_with_calendar
    conf_cross_id_trip_route_id_reference = db.Column(db.Boolean, default=False, nullable=False)  # trip_route_id_reference
    conf_cross_id_trip_service_id_reference = db.Column(db.Boolean, default=False, nullable=False)  # trip_service_id_reference
    conf_cross_id_stop_times_trip_id_reference = db.Column(db.Boolean, default=False, nullable=False)  # stop_times_trip_id_reference
    conf_cross_id_stop_times_stop_id_reference = db.Column(db.Boolean, default=False, nullable=False)  # stop_times_stop_id_reference
    conf_cross_id_fare_rules_route_id_reference = db.Column(db.Boolean, default=False, nullable=False)  # fare_rules_route_id_reference
    conf_cross_id_fare_rules_zone_id_reference = db.Column(db.Boolean, default=False, nullable=False)  # fare_rules_zone_id_reference
    conf_cross_id_frequencies_trip_id_reference = db.Column(db.Boolean, default=False, nullable=False)  # frequencies_trip_id_reference
    conf_cross_id_transfers_stop_id_reference = db.Column(db.Boolean, default=False, nullable=False)  # transfers_stop_id_reference
    conf_cross_id_shapes_shape_id_reference = db.Column(db.Boolean, default=False, nullable=False)  # shapes_shape_id_reference
    conf_cross_id_routes_usage_in_trips = db.Column(db.Boolean, default=False, nullable=False)  # routes_usage_in_trips
    conf_cross_id_trips_have_stop_times = db.Column(db.Boolean, default=False, nullable=False)  # trips_have_stop_times
    conf_cross_id_stops_usage_check = db.Column(db.Boolean, default=False, nullable=False)  # stops_usage_check
    conf_cross_id_shapes_usage_check = db.Column(db.Boolean, default=False, nullable=False)  # shapes_usage_check
    conf_cross_id_fare_attributes_usage = db.Column(db.Boolean, default=False, nullable=False)  # fare_attributes_usage
    conf_cross_id_calendar_services_not_used_in_trips = db.Column(db.Boolean, default=False, nullable=False)  # calendar_services_not_used_in_trips
    conf_cross_id_trips_with_invalid_route_id = db.Column(db.Boolean, default=False, nullable=False)  # trips_with_invalid_route_id
    conf_cross_id_calendar_dates_invalid_suppression = db.Column(db.Boolean, default=False, nullable=False)  # calendar_dates_invalid_suppression
    conf_cross_id_stop_times_with_missing_stop = db.Column(db.Boolean, default=False, nullable=False)  # stop_times_with_missing_stop
    conf_cross_id_fare_rules_with_invalid_fare_id = db.Column(db.Boolean, default=False, nullable=False)  # fare_rules_with_invalid_fare_id
    conf_cross_id_duplicate_ids_global = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_ids_global
    conf_cross_id_missing_primary_keys = db.Column(db.Boolean, default=False, nullable=False)  # missing_primary_keys
    conf_cross_validation_frequencies_no_time_overlap = db.Column(db.Boolean, default=False, nullable=False)  # frequencies_no_time_overlap
    conf_cross_validation_stop_times_within_frequencies_intervals = db.Column(db.Boolean, default=False, nullable=False)  # stop_times_within_frequencies_intervals
    conf_cross_validation_feed_info_date_coverage = db.Column(db.Boolean, default=False, nullable=False)  # feed_info_date_coverage
    conf_cross_validation_service_dates_within_feed_info = db.Column(db.Boolean, default=False, nullable=False)  # service_dates_within_feed_info
    conf_fare_attributes_duplicate_fare_attributes = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_fare_attributes
    conf_fare_attributes_fare_attributes_unused = db.Column(db.Boolean, default=False, nullable=False)  # fare_attributes_unused
    conf_file_duplicate_rows = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_rows
    conf_file_empty_values_stats = db.Column(db.Boolean, default=False, nullable=False)  # empty_values_stats
    conf_file_row_consistency = db.Column(db.Boolean, default=False, nullable=False)  # row_consistency
    conf_file_file_encoding = db.Column(db.Boolean, default=False, nullable=False)  # file_encoding
    conf_file_file_size = db.Column(db.Boolean, default=False, nullable=False)  # file_size
    conf_file_file_case_check = db.Column(db.Boolean, default=False, nullable=False)  # file_case_check
    conf_frequencies_required_fields_check = db.Column(db.Boolean, default=False, nullable=False)  # required_fields_check
    conf_frequencies_invalid_time_format = db.Column(db.Boolean, default=False, nullable=False)  # invalid_time_format
    conf_frequencies_start_after_end_check = db.Column(db.Boolean, default=False, nullable=False)  # start_after_end_check
    conf_frequencies_headway_outliers = db.Column(db.Boolean, default=False, nullable=False)  # headway_outliers
    conf_frequencies_overlapping_intervals = db.Column(db.Boolean, default=False, nullable=False)  # overlapping_intervals
    conf_frequencies_invalid_exact_times = db.Column(db.Boolean, default=False, nullable=False)  # invalid_exact_times
    conf_frequencies_frequencies_gap_analysis = db.Column(db.Boolean, default=False, nullable=False)  # frequencies_gap_analysis
    conf_frequencies_duplicate_frequencies_intervals = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_frequencies_intervals
    conf_geographic_shapes_cover_all_stops = db.Column(db.Boolean, default=False, nullable=False)  # shapes_cover_all_stops
    conf_geographic_stops_outliers_coordinates = db.Column(db.Boolean, default=False, nullable=False)  # stops_outliers_coordinates
    conf_geographic_distance_between_stops_consistency = db.Column(db.Boolean, default=False, nullable=False)  # distance_between_stops_consistency
    conf_geographic_duplicate_stops_geographically_close = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_stops_geographically_close
    conf_geographic_shape_orientation_check = db.Column(db.Boolean, default=False, nullable=False)  # shape_orientation_check
    conf_geographic_stops_far_from_shape_check = db.Column(db.Boolean, default=False, nullable=False)  # stops_far_from_shape_check
    conf_geographic_shape_point_distance_check = db.Column(db.Boolean, default=False, nullable=False)  # shape_point_distance_check
    conf_geographic_stops_outside_urban_area = db.Column(db.Boolean, default=False, nullable=False)  # stops_outside_urban_area
    conf_geographic_isolated_stops_detection = db.Column(db.Boolean, default=False, nullable=False)  # isolated_stops_detection
    conf_redondances_stop_id_coordinate_variation = db.Column(db.Boolean, default=False, nullable=False)  # stop_id_coordinate_variation
    conf_redondances_duplicate_trips = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_trips
    conf_routes_validate_route_colors = db.Column(db.Boolean, default=False, nullable=False)  # Validation couleurs
    conf_routes_check_route_id_uniqueness = db.Column(db.Boolean, default=False, nullable=False)  # check_route_id_uniqueness
    conf_routes_check_route_name_presence = db.Column(db.Boolean, default=False, nullable=False)  # check_route_name_presence
    conf_routes_check_duplicate_route_names = db.Column(db.Boolean, default=False, nullable=False)  # check_duplicate_route_names
    conf_routes_check_route_type_validity = db.Column(db.Boolean, default=False, nullable=False)  # check_route_type_validity
    conf_routes_check_route_color_format = db.Column(db.Boolean, default=False, nullable=False)  # check_route_color_format
    conf_routes_check_identical_route_names = db.Column(db.Boolean, default=False, nullable=False)  # check_identical_route_names
    conf_routes_check_route_url_validity = db.Column(db.Boolean, default=False, nullable=False)  # check_route_url_validity
    conf_routes_check_required_columns = db.Column(db.Boolean, default=False, nullable=False)  # check_required_columns
    conf_routes_route_id_uniqueness = db.Column(db.Boolean, default=False, nullable=False)  # route_id_uniqueness
    conf_routes_route_name_conflicts = db.Column(db.Boolean, default=False, nullable=False)  # route_name_conflicts
    conf_routes_missing_names = db.Column(db.Boolean, default=False, nullable=False)  # missing_names
    conf_routes_route_type_validity = db.Column(db.Boolean, default=False, nullable=False)  # route_type_validity
    conf_routes_route_color_format = db.Column(db.Boolean, default=False, nullable=False)  # route_color_format
    conf_routes_route_names_whitespace = db.Column(db.Boolean, default=False, nullable=False)  # route_names_whitespace
    conf_routes_long_name_contains_short_name = db.Column(db.Boolean, default=False, nullable=False)  # long_name_contains_short_name
    conf_routes_duplicate_routes_by_name_type = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_routes_by_name_type
    conf_routes_routes_without_trips = db.Column(db.Boolean, default=False, nullable=False)  # routes_without_trips
    conf_shapes_invalid_coordinates = db.Column(db.Boolean, default=False, nullable=False)  # invalid_coordinates
    conf_shapes_non_monotonic_sequences = db.Column(db.Boolean, default=False, nullable=False)  # non_monotonic_sequences
    conf_shapes_duplicate_points_in_shape = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_points_in_shape
    conf_shapes_minimal_distance_between_points = db.Column(db.Boolean, default=False, nullable=False)  # minimal_distance_between_points
    conf_shapes_shape_total_distance_stats = db.Column(db.Boolean, default=False, nullable=False)  # shape_total_distance_stats
    conf_shapes_closed_loop_shapes = db.Column(db.Boolean, default=False, nullable=False)  # closed_loop_shapes
    conf_shapes_uniform_spacing_detection = db.Column(db.Boolean, default=False, nullable=False)  # uniform_spacing_detection
    conf_shapes_abrupt_direction_changes = db.Column(db.Boolean, default=False, nullable=False)  # abrupt_direction_changes
    conf_shapes_shape_point_density = db.Column(db.Boolean, default=False, nullable=False)  # shape_point_density
    conf_shapes_backtracking_detection = db.Column(db.Boolean, default=False, nullable=False)  # backtracking_detection
    conf_shapes_shape_linearity_ratio = db.Column(db.Boolean, default=False, nullable=False)  # shape_linearity_ratio
    conf_shapes_consecutive_duplicate_points = db.Column(db.Boolean, default=False, nullable=False)  # consecutive_duplicate_points
    conf_shapes_isolated_shape_points = db.Column(db.Boolean, default=False, nullable=False)  # isolated_shape_points
    conf_shapes_similar_shapes_detection = db.Column(db.Boolean, default=False, nullable=False)  # similar_shapes_detection
    conf_shapes_shapes_bad_sequence = db.Column(db.Boolean, default=False, nullable=False)  # shapes_bad_sequence
    conf_shapes_shapes_large_jumps = db.Column(db.Boolean, default=False, nullable=False)  # shapes_large_jumps
    conf_stops_check_required_columns_stops = db.Column(db.Boolean, default=False, nullable=False)  # check_required_columns
    conf_stops_check_duplicate_stop_id = db.Column(db.Boolean, default=False, nullable=False)  # check_duplicate_stop_id
    conf_stops_validate_stop_lat_lon = db.Column(db.Boolean, default=False, nullable=False)  # validate_stop_lat_lon
    conf_stops_check_stop_name_uniqueness_per_location = db.Column(db.Boolean, default=False, nullable=False)  # check_stop_name_uniqueness_per_location
    conf_stops_missing_values_stats_stops = db.Column(db.Boolean, default=False, nullable=False)  # missing_values_stats
    conf_stops_validate_location_type = db.Column(db.Boolean, default=False, nullable=False)  # validate_location_type
    conf_stops_check_parent_station_consistency = db.Column(db.Boolean, default=False, nullable=False)  # check_parent_station_consistency
    conf_stops_validate_wheelchair_boarding = db.Column(db.Boolean, default=False, nullable=False)  # validate_wheelchair_boarding
    conf_stops_check_stop_code_format = db.Column(db.Boolean, default=False, nullable=False)  # check_stop_code_format
    conf_stops_detect_nearby_duplicate_stops = db.Column(db.Boolean, default=False, nullable=False)  # detect_nearby_duplicate_stops
    conf_stops_check_parent_station_cycles = db.Column(db.Boolean, default=False, nullable=False)  # check_parent_station_cycles
    conf_stops_check_stop_timezone_validity = db.Column(db.Boolean, default=False, nullable=False)  # check_stop_timezone_validity
    conf_stops_check_stop_url_validity = db.Column(db.Boolean, default=False, nullable=False)  # check_stop_url_validity
    conf_stops_analyze_stop_code_uniqueness = db.Column(db.Boolean, default=False, nullable=False)  # analyze_stop_code_uniqueness
    conf_stops_check_stop_desc_quality = db.Column(db.Boolean, default=False, nullable=False)  # check_stop_desc_quality
    conf_stops_stop_id_multiple_coords = db.Column(db.Boolean, default=False, nullable=False)  # stop_id_multiple_coords
    conf_stops_no_cycles_in_parent_station = db.Column(db.Boolean, default=False, nullable=False)  # no_cycles_in_parent_station
    conf_stops_children_zone_id_consistency = db.Column(db.Boolean, default=False, nullable=False)  # children_zone_id_consistency
    conf_stops_location_type_validity = db.Column(db.Boolean, default=False, nullable=False)  # location_type_validity
    conf_stops_parent_station_geographic_proximity = db.Column(db.Boolean, default=False, nullable=False)  # parent_station_geographic_proximity
    conf_stops_children_distance_stats = db.Column(db.Boolean, default=False, nullable=False)  # children_distance_stats
    conf_stops_children_municipality_consistency = db.Column(db.Boolean, default=False, nullable=False)  # children_municipality_consistency
    conf_stops_parent_station_stop_id_validity = db.Column(db.Boolean, default=False, nullable=False)  # parent_station_stop_id_validity
    conf_stops_stop_id_format_and_uniqueness = db.Column(db.Boolean, default=False, nullable=False)  # stop_id_format_and_uniqueness
    conf_stops_missing_coordinates_for_children = db.Column(db.Boolean, default=False, nullable=False)  # missing_coordinates_for_children
    conf_stops_isolated_stations = db.Column(db.Boolean, default=False, nullable=False)  # isolated_stations
    conf_stop_times_check_required_columns_stop_times = db.Column(db.Boolean, default=False, nullable=False)  # check_required_columns_stop_times
    conf_stop_times_check_arrival_before_departure = db.Column(db.Boolean, default=False, nullable=False)  # check_arrival_before_departure
    conf_stop_times_check_non_monotonic_times = db.Column(db.Boolean, default=False, nullable=False)  # check_non_monotonic_times
    conf_stop_times_check_headway_extremes = db.Column(db.Boolean, default=False, nullable=False)  # check_headway_extremes
    conf_stop_times_identify_zero_or_negative_durations = db.Column(db.Boolean, default=False, nullable=False)  # identify_zero_or_negative_durations
    conf_stop_times_check_stop_sequence_integrity = db.Column(db.Boolean, default=False, nullable=False)  # check_stop_sequence_integrity
    conf_stop_times_compute_average_stops_per_trip = db.Column(db.Boolean, default=False, nullable=False)  # compute_average_stops_per_trip
    conf_stop_times_identify_duplicate_stop_times = db.Column(db.Boolean, default=False, nullable=False)  # identify_duplicate_stop_times
    conf_stop_times_detect_extreme_time_gaps = db.Column(db.Boolean, default=False, nullable=False)  # detect_extreme_time_gaps
    conf_stop_times_detect_back_to_back_zero_wait = db.Column(db.Boolean, default=False, nullable=False)  # detect_back_to_back_zero_wait
    conf_stop_times_check_consecutive_arrival_dep_mismatch = db.Column(db.Boolean, default=False, nullable=False)  # check_consecutive_arrival_dep_mismatch
    conf_stop_times_analyze_trip_duration_extremes = db.Column(db.Boolean, default=False, nullable=False)  # analyze_trip_duration_extremes
    conf_stop_times_check_timepoint_flags_consistency = db.Column(db.Boolean, default=False, nullable=False)  # check_timepoint_flags_consistency
    conf_stop_times_detect_same_times_multiple_stops = db.Column(db.Boolean, default=False, nullable=False)  # detect_same_times_multiple_stops
    conf_stop_times_duplicate_stop_times_same_trip_same_stop_same_time = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_stop_times_same_trip_same_stop_same_time
    conf_stop_times_duplicate_stop_times_trip_stop = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_stop_times_trip_stop
    conf_stop_times_conflicting_stop_sequence_per_trip = db.Column(db.Boolean, default=False, nullable=False)  # conflicting_stop_sequence_per_trip
    conf_stop_times_stop_times_temporal_order = db.Column(db.Boolean, default=False, nullable=False)  # stop_times_temporal_order
    conf_stop_times_stop_times_non_negative_dwell = db.Column(db.Boolean, default=False, nullable=False)  # stop_times_non_negative_dwell
    conf_stop_times_stop_times_time_over_48h = db.Column(db.Boolean, default=False, nullable=False)  # stop_times_time_over_48h
    conf_transfers_invalid_stop_ids = db.Column(db.Boolean, default=False, nullable=False)  # invalid_stop_ids
    conf_transfers_invalid_transfer_type_values = db.Column(db.Boolean, default=False, nullable=False)  # invalid_transfer_type_values
    conf_transfers_duplicate_transfers = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_transfers
    conf_transfers_missing_min_transfer_time = db.Column(db.Boolean, default=False, nullable=False)  # missing_min_transfer_time
    conf_transfers_missing_symmetric_transfers = db.Column(db.Boolean, default=False, nullable=False)  # missing_symmetric_transfers
    conf_transfers_duplicate_transfers_pairs = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_transfers_pairs
    conf_trips_check_trip_id_uniqueness = db.Column(db.Boolean, default=False, nullable=False)  # check_trip_id_uniqueness
    conf_trips_check_required_columns_trips = db.Column(db.Boolean, default=False, nullable=False)  # check_required_columns
    conf_trips_headsign_completion_rate = db.Column(db.Boolean, default=False, nullable=False)  # headsign_completion_rate
    conf_trips_validate_direction_id = db.Column(db.Boolean, default=False, nullable=False)  # validate_direction_id
    conf_trips_shape_id_distribution = db.Column(db.Boolean, default=False, nullable=False)  # shape_id_distribution
    conf_trips_trips_without_shape = db.Column(db.Boolean, default=False, nullable=False)  # trips_without_shape
    conf_trips_service_id_variability = db.Column(db.Boolean, default=False, nullable=False)  # service_id_variability
    conf_trips_trip_name_field_completeness = db.Column(db.Boolean, default=False, nullable=False)  # trip_name_field_completeness
    conf_trips_trip_headsign_analysis = db.Column(db.Boolean, default=False, nullable=False)  # trip_headsign_analysis
    conf_trips_direction_variability_by_route = db.Column(db.Boolean, default=False, nullable=False)  # direction_variability_by_route
    conf_trips_trip_id_format_check = db.Column(db.Boolean, default=False, nullable=False)  # trip_id_format_check
    conf_trips_trip_id_entropy = db.Column(db.Boolean, default=False, nullable=False)  # trip_id_entropy
    conf_trips_redundant_trip_names = db.Column(db.Boolean, default=False, nullable=False)  # redundant_trip_names
    conf_trips_duplicate_trip_rows_without_id = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_trip_rows_without_id
    conf_trips_duplicate_trip_ids = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_trip_ids
    conf_trips_conflicting_trip_id_contexts = db.Column(db.Boolean, default=False, nullable=False)  # conflicting_trip_id_contexts
    conf_trips_duplicate_trip_ids = db.Column(db.Boolean, default=False, nullable=False)  # duplicate_trip_ids
    conf_za_stations_without_children = db.Column(db.Boolean, default=False, nullable=False)  # stations_without_children
    conf_za_stops_with_invalid_parent_station = db.Column(db.Boolean, default=False, nullable=False)  # stops_with_invalid_parent_station
    conf_za_child_stops_missing_parent_station_field = db.Column(db.Boolean, default=False, nullable=False)  # child_stops_missing_parent_station_field
    conf_za_stations_with_children_too_far = db.Column(db.Boolean, default=False, nullable=False)  # stations_with_children_too_far
    conf_za_children_stops_same_location_as_parent = db.Column(db.Boolean, default=False, nullable=False)  # children_stops_same_location_as_parent
    conf_za_stations_with_parent_station_defined = db.Column(db.Boolean, default=False, nullable=False)  # stations_with_parent_station_defined
    

    # Relation
    
    def to_dict(self):
        """Convertit l'objet en dictionnaire"""
        return {
            'config_id': self.config_id,
            'project_id': self.project_id,
            'date_configuration': self.date_configuration.isoformat() if self.date_configuration else None,
            'conf_agency_completion_phone': self.conf_agency_completion_phone,
            'conf_routes_validate_route_colors': self.conf_routes_validate_route_colors,
            'conf_agency_completion_url': self.conf_agency_completion_url,  # Complétude URL
            'conf_agency_completion_phone': self.conf_agency_completion_phone,  # Complétude téléphone
            'conf_agency_check_required_columns': self.conf_agency_check_required_columns,  # check_required_columns
            'conf_agency_validate_agency_url': self.conf_agency_validate_agency_url,  # validate_agency_url
            'conf_agency_validate_agency_timezone': self.conf_agency_validate_agency_timezone,  # validate_agency_timezone
            'conf_agency_check_duplicate_agency_id': self.conf_agency_check_duplicate_agency_id,  # check_duplicate_agency_id
            'conf_agency_missing_values_stats': self.conf_agency_missing_values_stats,  # missing_values_stats
            'conf_agency_check_field_length': self.conf_agency_check_field_length,  # check_field_length
            'conf_agency_validate_agency_lang': self.conf_agency_validate_agency_lang,  # validate_agency_lang
            'conf_agency_validate_agency_phone': self.conf_agency_validate_agency_phone,  # validate_agency_phone
            'conf_agency_validate_agency_fare_url': self.conf_agency_validate_agency_fare_url,  # validate_agency_fare_url
            'conf_agency_check_agency_consistency': self.conf_agency_check_agency_consistency,  # check_agency_consistency
            'conf_agency_duplicate_agencies_by_name_contact': self.conf_agency_duplicate_agencies_by_name_contact,  # duplicate_agencies_by_name_contact
            'conf_calendar_invalid_or_inverted_dates': self.conf_calendar_invalid_or_inverted_dates,  # invalid_or_inverted_dates
            'conf_calendar_inactive_services': self.conf_calendar_inactive_services,  # inactive_services
            'conf_calendar_excessive_duration_services': self.conf_calendar_excessive_duration_services,  # excessive_duration_services
            'conf_calendar_calendar_dates_service_not_in_calendar': self.conf_calendar_calendar_dates_service_not_in_calendar,  # calendar_dates_service_not_in_calendar
            'conf_calendar_exceptions_outside_date_range': self.conf_calendar_exceptions_outside_date_range,  # exceptions_outside_date_range
            'conf_calendar_never_active_services': self.conf_calendar_never_active_services,  # never_active_services
            'conf_calendar_duplicate_service_id_days': self.conf_calendar_duplicate_service_id_days,  # duplicate_service_id_days
            'conf_calendar_dates_invalid_dates': self.conf_calendar_dates_invalid_dates,  # invalid_dates
            'conf_calendar_dates_invalid_exception_types': self.conf_calendar_dates_invalid_exception_types,  # invalid_exception_types
            'conf_calendar_dates_duplicate_calendar_dates': self.conf_calendar_dates_duplicate_calendar_dates,  # duplicate_calendar_dates
            'conf_calendar_dates_conflicting_exceptions': self.conf_calendar_dates_conflicting_exceptions,  # conflicting_exceptions
            'conf_calendar_dates_calendar_dates_conflicts_with_calendar': self.conf_calendar_dates_calendar_dates_conflicts_with_calendar,  # calendar_dates_conflicts_with_calendar
            'conf_cross_id_trip_route_id_reference': self.conf_cross_id_trip_route_id_reference,  # trip_route_id_reference
            'conf_cross_id_trip_service_id_reference': self.conf_cross_id_trip_service_id_reference,  # trip_service_id_reference
            'conf_cross_id_stop_times_trip_id_reference': self.conf_cross_id_stop_times_trip_id_reference,  # stop_times_trip_id_reference
            'conf_cross_id_stop_times_stop_id_reference': self.conf_cross_id_stop_times_stop_id_reference,  # stop_times_stop_id_reference
            'conf_cross_id_fare_rules_route_id_reference': self.conf_cross_id_fare_rules_route_id_reference,  # fare_rules_route_id_reference
            'conf_cross_id_fare_rules_zone_id_reference': self.conf_cross_id_fare_rules_zone_id_reference,  # fare_rules_zone_id_reference
            'conf_cross_id_frequencies_trip_id_reference': self.conf_cross_id_frequencies_trip_id_reference,  # frequencies_trip_id_reference
            'conf_cross_id_transfers_stop_id_reference': self.conf_cross_id_transfers_stop_id_reference,  # transfers_stop_id_reference
            'conf_cross_id_shapes_shape_id_reference': self.conf_cross_id_shapes_shape_id_reference,  # shapes_shape_id_reference
            'conf_cross_id_routes_usage_in_trips': self.conf_cross_id_routes_usage_in_trips,  # routes_usage_in_trips
            'conf_cross_id_trips_have_stop_times': self.conf_cross_id_trips_have_stop_times,  # trips_have_stop_times
            'conf_cross_id_stops_usage_check': self.conf_cross_id_stops_usage_check,  # stops_usage_check
            'conf_cross_id_shapes_usage_check': self.conf_cross_id_shapes_usage_check,  # shapes_usage_check
            'conf_cross_id_fare_attributes_usage': self.conf_cross_id_fare_attributes_usage,  # fare_attributes_usage
            'conf_cross_id_calendar_services_not_used_in_trips': self.conf_cross_id_calendar_services_not_used_in_trips,  # calendar_services_not_used_in_trips
            'conf_cross_id_trips_with_invalid_route_id': self.conf_cross_id_trips_with_invalid_route_id,  # trips_with_invalid_route_id
            'conf_cross_id_calendar_dates_invalid_suppression': self.conf_cross_id_calendar_dates_invalid_suppression,  # calendar_dates_invalid_suppression
            'conf_cross_id_stop_times_with_missing_stop': self.conf_cross_id_stop_times_with_missing_stop,  # stop_times_with_missing_stop
            'conf_cross_id_fare_rules_with_invalid_fare_id': self.conf_cross_id_fare_rules_with_invalid_fare_id,  # fare_rules_with_invalid_fare_id
            'conf_cross_id_duplicate_ids_global': self.conf_cross_id_duplicate_ids_global,  # duplicate_ids_global
            'conf_cross_id_missing_primary_keys': self.conf_cross_id_missing_primary_keys,  # missing_primary_keys
            'conf_cross_validation_frequencies_no_time_overlap': self.conf_cross_validation_frequencies_no_time_overlap,  # frequencies_no_time_overlap
            'conf_cross_validation_stop_times_within_frequencies_intervals': self.conf_cross_validation_stop_times_within_frequencies_intervals,  # stop_times_within_frequencies_intervals
            'conf_cross_validation_feed_info_date_coverage': self.conf_cross_validation_feed_info_date_coverage,  # feed_info_date_coverage
            'conf_cross_validation_service_dates_within_feed_info': self.conf_cross_validation_service_dates_within_feed_info,  # service_dates_within_feed_info
            'conf_fare_attributes_duplicate_fare_attributes': self.conf_fare_attributes_duplicate_fare_attributes,  # duplicate_fare_attributes
            'conf_fare_attributes_fare_attributes_unused': self.conf_fare_attributes_fare_attributes_unused,  # fare_attributes_unused
            'conf_file_duplicate_rows': self.conf_file_duplicate_rows,  # duplicate_rows
            'conf_file_empty_values_stats': self.conf_file_empty_values_stats,  # empty_values_stats
            'conf_file_row_consistency': self.conf_file_row_consistency,  # row_consistency
            'conf_file_file_encoding': self.conf_file_file_encoding,  # file_encoding
            'conf_file_file_size': self.conf_file_file_size,  # file_size
            'conf_file_file_case_check': self.conf_file_file_case_check,  # file_case_check
            'conf_frequencies_required_fields_check': self.conf_frequencies_required_fields_check,  # required_fields_check
            'conf_frequencies_invalid_time_format': self.conf_frequencies_invalid_time_format,  # invalid_time_format
            'conf_frequencies_start_after_end_check': self.conf_frequencies_start_after_end_check,  # start_after_end_check
            'conf_frequencies_headway_outliers': self.conf_frequencies_headway_outliers,  # headway_outliers
            'conf_frequencies_overlapping_intervals': self.conf_frequencies_overlapping_intervals,  # overlapping_intervals
            'conf_frequencies_invalid_exact_times': self.conf_frequencies_invalid_exact_times,  # invalid_exact_times
            'conf_frequencies_frequencies_gap_analysis': self.conf_frequencies_frequencies_gap_analysis,  # frequencies_gap_analysis
            'conf_frequencies_duplicate_frequencies_intervals': self.conf_frequencies_duplicate_frequencies_intervals,  # duplicate_frequencies_intervals
            'conf_geographic_shapes_cover_all_stops': self.conf_geographic_shapes_cover_all_stops,  # shapes_cover_all_stops
            'conf_geographic_stops_outliers_coordinates': self.conf_geographic_stops_outliers_coordinates,  # stops_outliers_coordinates
            'conf_geographic_distance_between_stops_consistency': self.conf_geographic_distance_between_stops_consistency,  # distance_between_stops_consistency
            'conf_geographic_duplicate_stops_geographically_close': self.conf_geographic_duplicate_stops_geographically_close,  # duplicate_stops_geographically_close
            'conf_geographic_shape_orientation_check': self.conf_geographic_shape_orientation_check,  # shape_orientation_check
            'conf_geographic_stops_far_from_shape_check': self.conf_geographic_stops_far_from_shape_check,  # stops_far_from_shape_check
            'conf_geographic_shape_point_distance_check': self.conf_geographic_shape_point_distance_check,  # shape_point_distance_check
            'conf_geographic_stops_outside_urban_area': self.conf_geographic_stops_outside_urban_area,  # stops_outside_urban_area
            'conf_geographic_isolated_stops_detection': self.conf_geographic_isolated_stops_detection,  # isolated_stops_detection
            'conf_redondances_stop_id_coordinate_variation': self.conf_redondances_stop_id_coordinate_variation,  # stop_id_coordinate_variation
            'conf_redondances_duplicate_trips': self.conf_redondances_duplicate_trips,  # duplicate_trips
            'conf_routes_validate_route_colors': self.conf_routes_validate_route_colors,  # Validation couleurs
            'conf_routes_check_route_id_uniqueness': self.conf_routes_check_route_id_uniqueness,  # check_route_id_uniqueness
            'conf_routes_check_route_name_presence': self.conf_routes_check_route_name_presence,  # check_route_name_presence
            'conf_routes_check_duplicate_route_names': self.conf_routes_check_duplicate_route_names,  # check_duplicate_route_names
            'conf_routes_check_route_type_validity': self.conf_routes_check_route_type_validity,  # check_route_type_validity
            'conf_routes_check_route_color_format': self.conf_routes_check_route_color_format,  # check_route_color_format
            'conf_routes_check_identical_route_names': self.conf_routes_check_identical_route_names,  # check_identical_route_names
            'conf_routes_check_route_url_validity': self.conf_routes_check_route_url_validity,  # check_route_url_validity
            'conf_routes_check_required_columns': self.conf_routes_check_required_columns,  # check_required_columns
            'conf_routes_route_id_uniqueness': self.conf_routes_route_id_uniqueness,  # route_id_uniqueness
            'conf_routes_route_name_conflicts': self.conf_routes_route_name_conflicts,  # route_name_conflicts
            'conf_routes_missing_names': self.conf_routes_missing_names,  # missing_names
            'conf_routes_route_type_validity': self.conf_routes_route_type_validity,  # route_type_validity
            'conf_routes_route_color_format': self.conf_routes_route_color_format,  # route_color_format
            'conf_routes_route_names_whitespace': self.conf_routes_route_names_whitespace,  # route_names_whitespace
            'conf_routes_long_name_contains_short_name': self.conf_routes_long_name_contains_short_name,  # long_name_contains_short_name
            'conf_routes_duplicate_routes_by_name_type': self.conf_routes_duplicate_routes_by_name_type,  # duplicate_routes_by_name_type
            'conf_routes_routes_without_trips': self.conf_routes_routes_without_trips,  # routes_without_trips
            'conf_shapes_invalid_coordinates': self.conf_shapes_invalid_coordinates,  # invalid_coordinates
            'conf_shapes_non_monotonic_sequences': self.conf_shapes_non_monotonic_sequences,  # non_monotonic_sequences
            'conf_shapes_duplicate_points_in_shape': self.conf_shapes_duplicate_points_in_shape,  # duplicate_points_in_shape
            'conf_shapes_minimal_distance_between_points': self.conf_shapes_minimal_distance_between_points,  # minimal_distance_between_points
            'conf_shapes_shape_total_distance_stats': self.conf_shapes_shape_total_distance_stats,  # shape_total_distance_stats
            'conf_shapes_closed_loop_shapes': self.conf_shapes_closed_loop_shapes,  # closed_loop_shapes
            'conf_shapes_uniform_spacing_detection': self.conf_shapes_uniform_spacing_detection,  # uniform_spacing_detection
            'conf_shapes_abrupt_direction_changes': self.conf_shapes_abrupt_direction_changes,  # abrupt_direction_changes
            'conf_shapes_shape_point_density': self.conf_shapes_shape_point_density,  # shape_point_density
            'conf_shapes_backtracking_detection': self.conf_shapes_backtracking_detection,  # backtracking_detection
            'conf_shapes_shape_linearity_ratio': self.conf_shapes_shape_linearity_ratio,  # shape_linearity_ratio
            'conf_shapes_consecutive_duplicate_points': self.conf_shapes_consecutive_duplicate_points,  # consecutive_duplicate_points
            'conf_shapes_isolated_shape_points': self.conf_shapes_isolated_shape_points,  # isolated_shape_points
            'conf_shapes_similar_shapes_detection': self.conf_shapes_similar_shapes_detection,  # similar_shapes_detection
            'conf_shapes_shapes_bad_sequence': self.conf_shapes_shapes_bad_sequence,  # shapes_bad_sequence
            'conf_shapes_shapes_large_jumps': self.conf_shapes_shapes_large_jumps,  # shapes_large_jumps
            'conf_stops_check_required_columns_stops': self.conf_stops_check_required_columns_stops,  # check_required_columns
            'conf_stops_check_duplicate_stop_id': self.conf_stops_check_duplicate_stop_id,  # check_duplicate_stop_id
            'conf_stops_validate_stop_lat_lon': self.conf_stops_validate_stop_lat_lon,  # validate_stop_lat_lon
            'conf_stops_check_stop_name_uniqueness_per_location': self.conf_stops_check_stop_name_uniqueness_per_location,  # check_stop_name_uniqueness_per_location
            'conf_stops_missing_values_stats_stops': self.conf_stops_missing_values_stats_stops,  # missing_values_stats
            'conf_stops_validate_location_type': self.conf_stops_validate_location_type,  # validate_location_type
            'conf_stops_check_parent_station_consistency': self.conf_stops_check_parent_station_consistency,  # check_parent_station_consistency
            'conf_stops_validate_wheelchair_boarding': self.conf_stops_validate_wheelchair_boarding,  # validate_wheelchair_boarding
            'conf_stops_check_stop_code_format': self.conf_stops_check_stop_code_format,  # check_stop_code_format
            'conf_stops_detect_nearby_duplicate_stops': self.conf_stops_detect_nearby_duplicate_stops,  # detect_nearby_duplicate_stops
            'conf_stops_check_parent_station_cycles': self.conf_stops_check_parent_station_cycles,  # check_parent_station_cycles
            'conf_stops_check_stop_timezone_validity': self.conf_stops_check_stop_timezone_validity,  # check_stop_timezone_validity
            'conf_stops_check_stop_url_validity': self.conf_stops_check_stop_url_validity,  # check_stop_url_validity
            'conf_stops_analyze_stop_code_uniqueness': self.conf_stops_analyze_stop_code_uniqueness,  # analyze_stop_code_uniqueness
            'conf_stops_check_stop_desc_quality': self.conf_stops_check_stop_desc_quality,  # check_stop_desc_quality
            'conf_stops_stop_id_multiple_coords': self.conf_stops_stop_id_multiple_coords,  # stop_id_multiple_coords
            'conf_stops_no_cycles_in_parent_station': self.conf_stops_no_cycles_in_parent_station,  # no_cycles_in_parent_station
            'conf_stops_children_zone_id_consistency': self.conf_stops_children_zone_id_consistency,  # children_zone_id_consistency
            'conf_stops_location_type_validity': self.conf_stops_location_type_validity,  # location_type_validity
            'conf_stops_parent_station_geographic_proximity': self.conf_stops_parent_station_geographic_proximity,  # parent_station_geographic_proximity
            'conf_stops_children_distance_stats': self.conf_stops_children_distance_stats,  # children_distance_stats
            'conf_stops_children_municipality_consistency': self.conf_stops_children_municipality_consistency,  # children_municipality_consistency
            'conf_stops_parent_station_stop_id_validity': self.conf_stops_parent_station_stop_id_validity,  # parent_station_stop_id_validity
            'conf_stops_stop_id_format_and_uniqueness': self.conf_stops_stop_id_format_and_uniqueness,  # stop_id_format_and_uniqueness
            'conf_stops_missing_coordinates_for_children': self.conf_stops_missing_coordinates_for_children,  # missing_coordinates_for_children
            'conf_stops_isolated_stations': self.conf_stops_isolated_stations,  # isolated_stations
            'conf_stop_times_check_required_columns_stop_times': self.conf_stop_times_check_required_columns_stop_times,  # check_required_columns_stop_times
            'conf_stop_times_check_arrival_before_departure': self.conf_stop_times_check_arrival_before_departure,  # check_arrival_before_departure
            'conf_stop_times_check_non_monotonic_times': self.conf_stop_times_check_non_monotonic_times,  # check_non_monotonic_times
            'conf_stop_times_check_headway_extremes': self.conf_stop_times_check_headway_extremes,  # check_headway_extremes
            'conf_stop_times_identify_zero_or_negative_durations': self.conf_stop_times_identify_zero_or_negative_durations,  # identify_zero_or_negative_durations
            'conf_stop_times_check_stop_sequence_integrity': self.conf_stop_times_check_stop_sequence_integrity,  # check_stop_sequence_integrity
            'conf_stop_times_compute_average_stops_per_trip': self.conf_stop_times_compute_average_stops_per_trip,  # compute_average_stops_per_trip
            'conf_stop_times_identify_duplicate_stop_times': self.conf_stop_times_identify_duplicate_stop_times,  # identify_duplicate_stop_times
            'conf_stop_times_detect_extreme_time_gaps': self.conf_stop_times_detect_extreme_time_gaps,  # detect_extreme_time_gaps
            'conf_stop_times_detect_back_to_back_zero_wait': self.conf_stop_times_detect_back_to_back_zero_wait,  # detect_back_to_back_zero_wait
            'conf_stop_times_check_consecutive_arrival_dep_mismatch': self.conf_stop_times_check_consecutive_arrival_dep_mismatch,  # check_consecutive_arrival_dep_mismatch
            'conf_stop_times_analyze_trip_duration_extremes': self.conf_stop_times_analyze_trip_duration_extremes,  # analyze_trip_duration_extremes
            'conf_stop_times_check_timepoint_flags_consistency': self.conf_stop_times_check_timepoint_flags_consistency,  # check_timepoint_flags_consistency
            'conf_stop_times_detect_same_times_multiple_stops': self.conf_stop_times_detect_same_times_multiple_stops,  # detect_same_times_multiple_stops
            'conf_stop_times_duplicate_stop_times_same_trip_same_stop_same_time': self.conf_stop_times_duplicate_stop_times_same_trip_same_stop_same_time,  # duplicate_stop_times_same_trip_same_stop_same_time
            'conf_stop_times_duplicate_stop_times_trip_stop': self.conf_stop_times_duplicate_stop_times_trip_stop,  # duplicate_stop_times_trip_stop
            'conf_stop_times_conflicting_stop_sequence_per_trip': self.conf_stop_times_conflicting_stop_sequence_per_trip,  # conflicting_stop_sequence_per_trip
            'conf_stop_times_stop_times_temporal_order': self.conf_stop_times_stop_times_temporal_order,  # stop_times_temporal_order
            'conf_stop_times_stop_times_non_negative_dwell': self.conf_stop_times_stop_times_non_negative_dwell,  # stop_times_non_negative_dwell
            'conf_stop_times_stop_times_time_over_48h': self.conf_stop_times_stop_times_time_over_48h,  # stop_times_time_over_48h
            'conf_transfers_invalid_stop_ids': self.conf_transfers_invalid_stop_ids,  # invalid_stop_ids
            'conf_transfers_invalid_transfer_type_values': self.conf_transfers_invalid_transfer_type_values,  # invalid_transfer_type_values
            'conf_transfers_duplicate_transfers': self.conf_transfers_duplicate_transfers,  # duplicate_transfers
            'conf_transfers_missing_min_transfer_time': self.conf_transfers_missing_min_transfer_time,  # missing_min_transfer_time
            'conf_transfers_missing_symmetric_transfers': self.conf_transfers_missing_symmetric_transfers,  # missing_symmetric_transfers
            'conf_transfers_duplicate_transfers_pairs': self.conf_transfers_duplicate_transfers_pairs,  # duplicate_transfers_pairs
            'conf_trips_check_trip_id_uniqueness': self.conf_trips_check_trip_id_uniqueness,  # check_trip_id_uniqueness
            'conf_trips_check_required_columns_trips': self.conf_trips_check_required_columns_trips,  # check_required_columns
            'conf_trips_headsign_completion_rate': self.conf_trips_headsign_completion_rate,  # headsign_completion_rate
            'conf_trips_validate_direction_id': self.conf_trips_validate_direction_id,  # validate_direction_id
            'conf_trips_shape_id_distribution': self.conf_trips_shape_id_distribution,  # shape_id_distribution
            'conf_trips_trips_without_shape': self.conf_trips_trips_without_shape,  # trips_without_shape
            'conf_trips_service_id_variability': self.conf_trips_service_id_variability,  # service_id_variability
            'conf_trips_trip_name_field_completeness': self.conf_trips_trip_name_field_completeness,  # trip_name_field_completeness
            'conf_trips_trip_headsign_analysis': self.conf_trips_trip_headsign_analysis,  # trip_headsign_analysis
            'conf_trips_direction_variability_by_route': self.conf_trips_direction_variability_by_route,  # direction_variability_by_route
            'conf_trips_trip_id_format_check': self.conf_trips_trip_id_format_check,  # trip_id_format_check
            'conf_trips_trip_id_entropy': self.conf_trips_trip_id_entropy,  # trip_id_entropy
            'conf_trips_redundant_trip_names': self.conf_trips_redundant_trip_names,  # redundant_trip_names
            'conf_trips_duplicate_trip_rows_without_id': self.conf_trips_duplicate_trip_rows_without_id,  # duplicate_trip_rows_without_id
            'conf_trips_duplicate_trip_ids': self.conf_trips_duplicate_trip_ids,  # duplicate_trip_ids
            'conf_trips_conflicting_trip_id_contexts': self.conf_trips_conflicting_trip_id_contexts,  # conflicting_trip_id_contexts
            'conf_za_stations_without_children': self.conf_za_stations_without_children,  # stations_without_children
            'conf_za_stops_with_invalid_parent_station': self.conf_za_stops_with_invalid_parent_station,  # stops_with_invalid_parent_station
            'conf_za_child_stops_missing_parent_station_field': self.conf_za_child_stops_missing_parent_station_field,  # child_stops_missing_parent_station_field
            'conf_za_stations_with_children_too_far': self.conf_za_stations_with_children_too_far,  # stations_with_children_too_far
            'conf_za_children_stops_same_location_as_parent': self.conf_za_children_stops_same_location_as_parent,  # children_stops_same_location_as_parent
            'conf_za_stations_with_parent_station_defined': self.conf_za_stations_with_parent_station_defined,  # stations_with_parent_station_defined
        }