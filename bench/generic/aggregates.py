from django.db.models.aggregates import Aggregate


class ClickhouseAggregate(Aggregate):
	pass


class GroupArray(Aggregate):
	function = 'groupArray'
	name = 'Group array'
	empty_result_set_value = []		# type: ignore # This function works with generic type or types
