<?php

	// template
	$prefix = "create(";
	$suffix = ',"unsorted")';
	$insert = "t1";
	$letter = 97;

	// do all 26 letters
	for ($i = 0; $i < 26; $i++)
		echo $prefix . $insert . chr($letter + $i) . $suffix . PHP_EOL;